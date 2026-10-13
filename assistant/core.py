"""Agent conversationnel Vélib' : LLM via OpenRouter + outils SQL en lecture seule sur le schéma gold.

Ce module ne dépend ni de FastAPI ni d'une base réelle : l'appel au LLM et l'accès aux données sont
injectés dans run_agent(), ce qui permet de le tester sans réseau.
"""

import json
import logging
import os
import re
from datetime import datetime, timezone

logger = logging.getLogger("assistant")

OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
# Un ou plusieurs modèles séparés par des virgules : on passe au suivant si l'un est saturé ou indisponible.
DEFAULT_MODELS = "google/gemma-4-31b-it:free,google/gemma-4-26b-a4b-it:free,nvidia/nemotron-3-super-120b-a12b:free"
OPENROUTER_MODELS = [m.strip() for m in os.getenv("OPENROUTER_MODEL", DEFAULT_MODELS).split(",") if m.strip()]
OPENROUTER_MODEL = OPENROUTER_MODELS[0]
MAX_ROWS = 40
MAX_ROUNDS = 5
MAX_TURNS = 12

SCHEMA_TABLES = [
    "etat_actuel_stations",
    "dim_station",
    "agg_stations_critiques",
    "agg_remplissage_horaire",
    "agg_disponibilite_zone_horaire",
    "dim_date",
    "dim_heure",
]

FORBIDDEN = re.compile(
    r"\b(insert|update|delete|merge|drop|alter|create|truncate|grant|revoke|copy|call|do|vacuum|analyze|"
    r"set|reset|lock|listen|notify|pg_sleep|pg_read_file|pg_ls_dir|dblink)\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Garde-fous SQL (le rôle assistant_ro est déjà en lecture seule : ceci est une 2e barrière)
# ---------------------------------------------------------------------------
def validate_sql(sql):
    """Renvoie la requête nettoyée si c'est un unique SELECT, sinon lève ValueError."""
    cleaned = re.sub(r"--[^\n]*", " ", str(sql or "")).strip().rstrip(";").strip()
    if not cleaned:
        raise ValueError("Requête vide.")
    if ";" in cleaned:
        raise ValueError("Une seule requête à la fois.")
    if not re.match(r"^(select|with)\b", cleaned, re.IGNORECASE):
        raise ValueError("Seules les requêtes SELECT sont autorisées.")
    if FORBIDDEN.search(cleaned):
        raise ValueError("Cette requête contient une instruction interdite : lecture seule uniquement.")
    return cleaned


def _jsonable(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float):
        return round(value, 3)
    if hasattr(value, "is_finite"):  # Decimal
        return round(float(value), 3)
    return value


# ---------------------------------------------------------------------------
# Outils exécutés par le serveur
# ---------------------------------------------------------------------------
class Warehouse:
    """Accès en lecture au schéma gold avec le rôle assistant_ro."""

    def __init__(self, dsn=None):
        self.dsn = dsn or "postgresql://{u}:{p}@{h}:{port}/{db}".format(
            u=os.getenv("ASSISTANT_DB_USER", "assistant_ro"),
            p=os.getenv("ASSISTANT_DB_PASSWORD", "assistant_ro_2026"),
            h=os.getenv("POSTGRES_HOST", "postgres"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            db=os.getenv("POSTGRES_DB", "velib_dw"),
        )

    def query(self, sql, params=None):
        import psycopg2

        with psycopg2.connect(self.dsn, connect_timeout=5) as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            columns = [d.name for d in cur.description]
            return columns, cur.fetchall()

    def run_sql(self, sql):
        cleaned = validate_sql(sql)
        columns, rows = self.query(f"SELECT * FROM ({cleaned}) AS q LIMIT {MAX_ROWS + 1}")
        return {
            "columns": columns,
            "rows": [[_jsonable(v) for v in r] for r in rows[:MAX_ROWS]],
            "row_count": len(rows[:MAX_ROWS]),
            "truncated": len(rows) > MAX_ROWS,
        }

    def stations_proches(self, lat, lon, n=5, besoin="tout"):
        lat, lon = float(lat), float(lon)
        n = min(max(int(n or 5), 1), 12)
        condition = {
            "velo": "AND velos_disponibles > 0",
            "electrique": "AND velos_electriques > 0",
            "borne": "AND bornes_libres > 0",
        }.get(besoin or "tout", "")
        columns, rows = self.query(
            f"""
            SELECT nom_station AS station, zone,
                   round((2 * 6371000 * asin(sqrt(
                       power(sin(radians(latitude - %(lat)s) / 2), 2)
                       + cos(radians(%(lat)s)) * cos(radians(latitude))
                         * power(sin(radians(longitude - %(lon)s) / 2), 2))))::numeric) AS distance_m,
                   velos_disponibles AS velos, velos_mecaniques AS mecaniques,
                   velos_electriques AS electriques, bornes_libres, statut
            FROM etat_actuel_stations
            WHERE statut <> 'hors service' {condition}
            ORDER BY distance_m
            LIMIT %(n)s
            """,
            {"lat": lat, "lon": lon, "n": n},
        )
        return [{c: _jsonable(v) for c, v in zip(columns, r)} for r in rows]

    def schema_text(self):
        columns, rows = self.query(
            """
            SELECT table_name, string_agg(column_name, ', ' ORDER BY ordinal_position)
            FROM information_schema.columns
            WHERE table_schema = 'gold' AND table_name = ANY(%s)
            GROUP BY table_name ORDER BY table_name
            """,
            (SCHEMA_TABLES,),
        )
        return "\n".join(f"- {t}({cols})" for t, cols in rows)

    def last_snapshot(self):
        _, rows = self.query("SELECT max(dernier_releve_at) FROM etat_actuel_stations")
        return rows[0][0]


TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "run_sql",
            "description": (
                "Exécute UNE requête PostgreSQL SELECT sur le schéma gold (tables listées dans les instructions) "
                f"et renvoie {{columns, rows ({MAX_ROWS} max), row_count}}. Pour compter, filtrer, classer, comparer."
            ),
            "parameters": {
                "type": "object",
                "properties": {"sql": {"type": "string", "description": "Requête PostgreSQL SELECT"}},
                "required": ["sql"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "stations_proches",
            "description": (
                "Stations les plus proches d'un point GPS, triées par distance (mètres), avec vélos mécaniques, "
                "électriques et bornettes libres. besoin = 'velo', 'electrique', 'borne' (place pour rendre) ou 'tout'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "lat": {"type": "number"},
                    "lon": {"type": "number"},
                    "n": {"type": "integer", "description": "1 à 12, défaut 5"},
                    "besoin": {"type": "string", "enum": ["velo", "electrique", "borne", "tout"]},
                    "lieu": {"type": "string", "description": "Nom du lieu recherché"},
                },
                "required": ["lat", "lon"],
            },
        },
    },
]


def system_prompt(schema_text, snapshot):
    if isinstance(snapshot, datetime):
        snapshot = snapshot.astimezone(timezone.utc).strftime("%d/%m/%Y %H:%M UTC")
    return f"""Tu es l'assistant client d'une plateforme de données sur les vélos en libre-service Vélib' \
(Paris et métropole). Projet de démonstration, non affilié à Vélib' Métropole.

Règles :
- Réponds en français, court, chaleureux et concret (2 à 6 lignes la plupart du temps).
- Appuie-toi UNIQUEMENT sur les résultats des outils. N'invente jamais un chiffre ni une station.
- Lieu ou adresse : estime toi-même ses coordonnées GPS, puis appelle stations_proches (renseigne "lieu").
  Les cartes des stations s'affichent sous ta réponse : recommande la meilleure option en une ou deux phrases
  avec la distance arrondie, sans recopier toute la liste.
- Comptage, classement, tendance : utilise run_sql (syntaxe PostgreSQL). Pour plusieurs lignes, un petit
  tableau Markdown (3 ou 4 colonnes) est bienvenu.
- Quand tu parles de disponibilité, précise l'heure du dernier relevé : {snapshot}.
- Hors sujet (autre chose que vélos, stations, mobilité dans la métropole) : dis poliment que tu ne peux aider
  que là-dessus.
- Abonnement, vélo en panne, facturation, réclamation : oriente vers le service client officiel de Vélib' Métropole.

Schéma gold (PostgreSQL) :
{schema_text}
statut dans etat_actuel_stations : 'disponible', 'presque vide', 'vide', 'presque pleine', 'pleine', 'hors service'.
zone : 'Paris 1er', 'Paris 2e' … 'Paris 20e', sinon le nom de la commune (ex. 'Montreuil')."""


# ---------------------------------------------------------------------------
# Appel au LLM (OpenRouter, API compatible OpenAI)
# ---------------------------------------------------------------------------
class LLMError(RuntimeError):
    pass


def _http_post(url, headers, payload, timeout):
    import requests

    response = requests.post(url, headers=headers, json=payload, timeout=timeout)
    try:
        body = response.json()
    except ValueError:
        body = {"error": {"message": response.text[:300]}}
    return response.status_code, body


def _is_daily_quota(body):
    """429 dû au quota de la clé (et non à la saturation d'un fournisseur) : inutile d'essayer un autre modèle."""
    meta = (body.get("error") or {}).get("metadata") or {}
    return "upstream" not in str(meta.get("limit_source", "")) and "upstream" not in str(meta.get("raw", ""))


def openrouter_chat(messages, tools=None, api_key=None, models=None, timeout=90, post=_http_post):
    """Appel /chat/completions avec repli sur les modèles suivants si l'un est saturé ou indisponible.

    Renvoie le message de l'assistant (dict au format OpenAI).
    """
    api_key = api_key or os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise LLMError("OPENROUTER_API_KEY n'est pas configurée.")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/adham-marrakchi/velib-data-platform",
        "X-Title": "Velib Data Platform",
    }
    failures = []
    for model in models or OPENROUTER_MODELS:
        payload = {"model": model, "messages": messages, "temperature": 0.2, **({"tools": tools} if tools else {})}
        try:
            status, body = post(f"{OPENROUTER_BASE_URL}/chat/completions", headers, payload, timeout)
        except Exception as exc:  # coupure réseau, délai dépassé
            failures.append(f"{model} : {exc}")
            continue
        if status == 401:
            raise LLMError("Clé OpenRouter refusée : vérifie OPENROUTER_API_KEY.")
        if status == 429 and _is_daily_quota(body):
            raise LLMError("Quota OpenRouter atteint (modèles gratuits : 50 requêtes par jour). Réessaie demain.")
        error = body.get("error")
        if status >= 400 or error or not body.get("choices"):
            message = (error or {}).get("message", f"HTTP {status}") if isinstance(error, dict) else str(error)
            failures.append(f"{model} : {message}")
            logger.warning("Modèle %s indisponible (%s), essai du suivant.", model, message)
            continue
        return body["choices"][0]["message"]
    raise LLMError(
        "Les modèles sont momentanément saturés. Réessaie dans une minute. (" + " ; ".join(failures)[:300] + ")"
    )


# ---------------------------------------------------------------------------
# Boucle de l'agent
# ---------------------------------------------------------------------------
def trim_history(turns, max_turns=MAX_TURNS):
    """Garde les derniers échanges user/assistant (texte seul), en commençant par un message user."""
    clean = [
        {"role": t["role"], "content": str(t["content"])[:4000]}
        for t in turns
        if t.get("role") in ("user", "assistant") and str(t.get("content", "")).strip()
    ][-max_turns:]
    while clean and clean[0]["role"] != "user":
        clean.pop(0)
    return clean


def run_agent(turns, llm, warehouse, system, max_rounds=MAX_ROUNDS):
    """Générateur d'événements : tool_start, tool_end, stations, answer, error.

    llm(messages, tools) -> message assistant ; warehouse expose run_sql() et stations_proches().
    """
    messages = [{"role": "system", "content": system}, *trim_history(turns)]
    if len(messages) < 2:
        yield {"type": "error", "message": "Aucune question reçue."}
        return

    for _ in range(max_rounds):
        try:
            reply = llm(messages, TOOLS)
        except Exception as exc:  # erreur réseau, quota, modèle indisponible
            logger.warning("Appel LLM en échec : %s", exc)
            yield {"type": "error", "message": str(exc)}
            return

        tool_calls = reply.get("tool_calls") or []
        if not tool_calls:
            yield {"type": "answer", "text": (reply.get("content") or "").strip() or "Je n'ai pas de réponse."}
            return

        messages.append({"role": "assistant", "content": reply.get("content") or "", "tool_calls": tool_calls})
        for call in tool_calls:
            name = call.get("function", {}).get("name")
            try:
                args = json.loads(call.get("function", {}).get("arguments") or "{}")
            except json.JSONDecodeError:
                args = {}
            yield {"type": "tool_start", "id": call.get("id"), "name": name, "args": args}
            try:
                if name == "run_sql":
                    result = warehouse.run_sql(args.get("sql"))
                    summary = f"{result['row_count']} ligne(s)"
                elif name == "stations_proches":
                    result = warehouse.stations_proches(
                        args.get("lat"), args.get("lon"), args.get("n", 5), args.get("besoin", "tout")
                    )
                    summary = f"{len(result)} station(s)"
                    yield {"type": "stations", "stations": result}
                else:
                    raise ValueError(f"Outil inconnu : {name}")
                content = json.dumps(result, ensure_ascii=False, default=str)
                yield {"type": "tool_end", "id": call.get("id"), "ok": True, "summary": summary}
            except Exception as exc:
                content = f"Erreur : {exc}"
                yield {"type": "tool_end", "id": call.get("id"), "ok": False, "summary": str(exc)[:200]}
            messages.append({"role": "tool", "tool_call_id": call.get("id"), "name": name, "content": content})

    yield {"type": "error", "message": "L'assistant n'a pas conclu à temps. Reformule ta question."}
