"""SILVER — job Spark batch : bronze (JSON brut) -> silver (typé, validé, dédoublonné).

Étapes :
  1. lecture de la fenêtre bronze [start - lookback, end[ (élagage par partition ingest_date) ;
  2. parsing du JSON avec un schéma explicite + typage (timestamps UTC, booléens, entiers) ;
  3. séparation vélos mécaniques / électriques ;
  4. règles de validité -> lignes invalides envoyées en QUARANTAINE avec leur motif ;
  5. dédoublonnage sur (station_id, last_reported) en gardant le dernier offset Kafka ;
  6. MERGE idempotent dans la table Delta silver (rejouer une fenêtre ne crée pas de doublon) ;
  7. ajout du lot dans PostgreSQL (staging.velib_status_incoming, vidée par le DAG) pour le contrôle qualité
     Great Expectations, avant fusion dans staging.velib_status.

Mode --maintenance : OPTIMIZE (compaction des petits fichiers du streaming) + VACUUM.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from lake_config import BRONZE_STATUS_PATH, QUARANTINE_STATUS_PATH, SILVER_STATUS_PATH, build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("silver-job")

PG_URL = "jdbc:postgresql://{host}:{port}/{db}".format(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    port=os.getenv("POSTGRES_PORT", "5432"),
    db=os.getenv("POSTGRES_DB", "velib_dw"),
)
PG_PROPS = {
    "user": os.getenv("POSTGRES_USER", "velib"),
    "password": os.getenv("POSTGRES_PASSWORD", "velib_secret_2026"),
    "driver": "org.postgresql.Driver",
}
MIN_VALID_EPOCH = 1577836800  # 2020-01-01 : en dessous, last_reported est une valeur par défaut

STAGING_COLUMNS = [
    "station_id",
    "station_code",
    "last_reported",
    "num_bikes_available",
    "num_mechanical",
    "num_ebike",
    "num_docks_available",
    "is_installed",
    "is_renting",
    "is_returning",
    "kafka_timestamp",
    "ingested_at",
]


def envelope_schema():
    from pyspark.sql.types import ArrayType, IntegerType, LongType, MapType, StringType, StructField, StructType

    station = StructType(
        [
            StructField("station_id", LongType()),
            StructField("stationCode", StringType()),
            StructField("num_bikes_available", IntegerType()),
            StructField("num_bikes_available_types", ArrayType(MapType(StringType(), IntegerType()))),
            StructField("num_docks_available", IntegerType()),
            StructField("is_installed", IntegerType()),
            StructField("is_renting", IntegerType()),
            StructField("is_returning", IntegerType()),
            StructField("last_reported", LongType()),
        ]
    )
    return StructType(
        [
            StructField("feed_last_updated", LongType()),
            StructField("fetched_at", StringType()),
            StructField("source", StringType()),
            StructField("station", station),
        ]
    )


def parse_bronze(df_bronze):
    """JSON brut -> colonnes typées. N'élimine aucune ligne (la validation est faite ensuite)."""
    from pyspark.sql import functions as F

    parsed = df_bronze.withColumn("env", F.from_json("raw_json", envelope_schema()))
    s = "env.station"
    count_type = (
        "aggregate(coalesce({s}.num_bikes_available_types, array()), 0, (acc, m) -> acc + coalesce(m['{t}'], 0))"
    )
    return parsed.select(
        F.col(f"{s}.station_id").alias("station_id"),
        F.col(f"{s}.stationCode").alias("station_code"),
        F.col(f"{s}.last_reported").alias("last_reported_epoch"),
        F.to_timestamp(F.from_unixtime(F.col(f"{s}.last_reported"))).alias("last_reported"),
        F.col(f"{s}.num_bikes_available").alias("num_bikes_available"),
        F.expr(count_type.format(s=s, t="mechanical")).cast("int").alias("num_mechanical"),
        F.expr(count_type.format(s=s, t="ebike")).cast("int").alias("num_ebike"),
        F.col(f"{s}.num_docks_available").alias("num_docks_available"),
        (F.col(f"{s}.is_installed") == 1).alias("is_installed"),
        (F.col(f"{s}.is_renting") == 1).alias("is_renting"),
        (F.col(f"{s}.is_returning") == 1).alias("is_returning"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_timestamp"),
        F.col("ingested_at"),
        F.col("raw_json"),
    )


def add_rejection_reason(df, known_station_ids=None, now_epoch=None):
    """Ajoute la colonne rejection_reason (null = ligne valide). Première règle violée retenue."""
    from pyspark.sql import functions as F

    now_epoch = now_epoch or int(datetime.now(timezone.utc).timestamp())
    reason = (
        F.when(F.col("station_id").isNull(), F.lit("json_invalide_ou_station_id_absent"))
        .when(F.col("last_reported_epoch").isNull(), F.lit("last_reported_absent"))
        .when(F.col("last_reported_epoch") < MIN_VALID_EPOCH, F.lit("last_reported_par_defaut"))
        .when(F.col("last_reported_epoch") > now_epoch + 3600, F.lit("last_reported_dans_le_futur"))
        .when(F.col("num_bikes_available").isNull() | (F.col("num_bikes_available") < 0), F.lit("velos_negatifs"))
        .when(F.col("num_docks_available").isNull() | (F.col("num_docks_available") < 0), F.lit("bornes_negatives"))
    )
    if known_station_ids is not None:
        reason = reason.when(~F.col("is_known_station"), F.lit("station_inconnue"))
        df = df.join(known_station_ids.withColumn("is_known_station", F.lit(True)), on="station_id", how="left").fillna(
            {"is_known_station": False}
        )
    df = df.withColumn("rejection_reason", reason)
    return df.drop("is_known_station") if "is_known_station" in df.columns else df


def deduplicate(df_valid):
    """Un relevé par (station_id, last_reported) : on garde le message le plus récent dans Kafka."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    window = Window.partitionBy("station_id", "last_reported").orderBy(
        F.col("kafka_timestamp").desc(), F.col("kafka_offset").desc()
    )
    return (
        df_valid.withColumn("_rn", F.row_number().over(window))
        .filter("_rn = 1")
        .drop("_rn", "raw_json", "rejection_reason", "last_reported_epoch")
        .withColumn("report_date", F.to_date("last_reported"))
        .withColumn("silver_processed_at", F.current_timestamp())
    )


def merge_into_silver(spark, df_silver):
    """MERGE idempotent : insère uniquement les relevés absents de silver."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, SILVER_STATUS_PATH):
        df_silver.write.format("delta").partitionBy("report_date").save(SILVER_STATUS_PATH)
        return
    (
        DeltaTable.forPath(spark, SILVER_STATUS_PATH)
        .alias("t")
        .merge(
            df_silver.alias("s"),
            "t.report_date = s.report_date AND t.station_id = s.station_id AND t.last_reported = s.last_reported",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def load_known_stations(spark):
    """Référentiel stations depuis PostgreSQL (alimenté par velib_stations_reference_dag)."""
    df = spark.read.jdbc(PG_URL, "(SELECT station_id FROM staging.velib_stations) AS s", properties=PG_PROPS)
    if df.limit(1).count() == 0:
        logger.warning("Référentiel stations vide : contrôle 'station inconnue' désactivé pour ce lot.")
        return None
    return df


def run_silver(spark, start, end, lookback_minutes):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    window_start = start - timedelta(minutes=lookback_minutes)
    stats = {"window_start": window_start.isoformat(), "window_end": end.isoformat()}

    if not DeltaTable.isDeltaTable(spark, BRONZE_STATUS_PATH):
        logger.warning("Table bronze absente (%s) : rien à traiter.", BRONZE_STATUS_PATH)
        stats.update(bronze_rows=0, valid_rows=0, rejected_rows=0)
        return stats

    bronze = (
        spark.read.format("delta")
        .load(BRONZE_STATUS_PATH)
        .filter(F.col("ingest_date").between(window_start.date().isoformat(), end.date().isoformat()))
        .filter((F.col("kafka_timestamp") >= F.lit(window_start)) & (F.col("kafka_timestamp") < F.lit(end)))
    )
    checked = add_rejection_reason(parse_bronze(bronze), load_known_stations(spark)).cache()

    rejected = checked.filter(F.col("rejection_reason").isNotNull())
    valid = deduplicate(checked.filter(F.col("rejection_reason").isNull())).cache()

    stats["bronze_rows"] = checked.count()
    stats["rejected_rows"] = rejected.count()
    stats["valid_rows"] = valid.count()
    stats["rejections_by_reason"] = {
        r["rejection_reason"]: r["count"] for r in rejected.groupBy("rejection_reason").count().collect()
    }

    if stats["rejected_rows"]:
        (
            rejected.select("raw_json", "rejection_reason", "kafka_partition", "kafka_offset", "kafka_timestamp")
            .withColumn("quarantined_at", F.current_timestamp())
            .withColumn("quarantine_date", F.current_date())
            .write.format("delta")
            .mode("append")
            .partitionBy("quarantine_date")
            .save(QUARANTINE_STATUS_PATH)
        )
    if stats["valid_rows"]:
        merge_into_silver(spark, valid)
        write_incoming(valid)
    return stats


def write_incoming(df):
    """Ajoute le lot dans staging.velib_status_incoming (vidée au préalable par le DAG Airflow)."""
    df.select(*STAGING_COLUMNS).write.jdbc(
        PG_URL, "staging.velib_status_incoming", mode="append", properties={**PG_PROPS, "batchsize": "5000"}
    )


def run_maintenance(spark, vacuum_hours):
    """Compaction (OPTIMIZE) des tables Delta et suppression des anciens fichiers (VACUUM)."""
    from delta.tables import DeltaTable

    for path in (BRONZE_STATUS_PATH, SILVER_STATUS_PATH):
        if DeltaTable.isDeltaTable(spark, path):
            table = DeltaTable.forPath(spark, path)
            metrics = table.optimize().executeCompaction().select("metrics.numFilesRemoved", "metrics.numFilesAdded")
            logger.info("OPTIMIZE %s : %s", path, metrics.collect())
            table.vacuum(vacuum_hours)
            logger.info("VACUUM %s (rétention %sh) terminé.", path, vacuum_hours)


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", help="Début de fenêtre ISO 8601 (défaut : end - 1h)")
    parser.add_argument("--end", help="Fin de fenêtre ISO 8601 (défaut : maintenant)")
    parser.add_argument("--lookback-minutes", type=int, default=30, help="Marge pour les messages tardifs")
    parser.add_argument("--maintenance", action="store_true", help="OPTIMIZE + VACUUM au lieu du traitement")
    parser.add_argument("--vacuum-hours", type=int, default=168)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv if argv is not None else sys.argv[1:])
    spark = build_spark_session("velib-silver", master=os.getenv("SPARK_BATCH_MASTER", "local[2]"))
    spark.sparkContext.setLogLevel("WARN")
    try:
        if args.maintenance:
            run_maintenance(spark, args.vacuum_hours)
            return
        end = datetime.fromisoformat(args.end) if args.end else datetime.now(timezone.utc)
        start = datetime.fromisoformat(args.start) if args.start else end - timedelta(hours=1)
        stats = run_silver(spark, start.astimezone(timezone.utc), end.astimezone(timezone.utc), args.lookback_minutes)
        logger.info("Bilan silver : %s", stats)
        # Dernière ligne de sortie = JSON (récupérée en XCom par le BashOperator)
        print(json.dumps(stats))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
