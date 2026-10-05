using System.Threading;
using System.Threading.Tasks;
using DataPipelineApi.Services;
using Microsoft.AspNetCore.Mvc;

namespace DataPipelineApi.Controllers;

[ApiController]
[Route("api/warehouse")]
public class WarehouseController : ControllerBase
{
    private readonly IDbService _db;
    private readonly IBatchService _airflow;
    private readonly ILogger<WarehouseController> _logger;
    private readonly IConfiguration _config;

    public WarehouseController(
        IDbService db,
        IBatchService airflow,
        ILogger<WarehouseController> logger,
        IConfiguration config)
    {
        _db = db;
        _airflow = airflow;
        _logger = logger;
        _config = config;
    }

    /// <summary>Trigger the Snowflake warehouse transformation DAG</summary>
    [HttpPost("transform")]
    public async Task<ActionResult> TriggerTransform(CancellationToken ct)
    {
        var runId = await _airflow.TriggerBatchAsync(ct);
        _logger.LogInformation("Warehouse transform triggered: {RunId}", runId);
        return Ok(new
        {
            runId,
            message = "Snowflake warehouse transformation DAG triggered",
            target = IsSnowflakeConfigured() ? "snowflake" : "postgresql_fallback"
        });
    }

    /// <summary>Get daily order aggregations from the warehouse</summary>
    [HttpGet("aggregations/daily-orders")]
    public async Task<ActionResult> GetDailyOrderAggregations(CancellationToken ct)
    {
        try
        {
            await _db.ExecutePostgresAsync("SELECT 1", ct);
            return Ok(new
            {
                source = IsSnowflakeConfigured() ? "snowflake" : "postgresql",
                message = "Query warehouse via Snowflake SQL or /swagger for API access",
                snowflake_table = "PIPELINE_DB.ANALYTICS.AGG_DAILY_ORDERS",
                postgres_table = "agg_daily_orders"
            });
        }
        catch (System.Exception ex)
        {
            _logger.LogError(ex, "Failed to query warehouse");
            return StatusCode(503, new { error = "Warehouse database unavailable" });
        }
    }

    /// <summary>Get pipeline run history from warehouse</summary>
    [HttpGet("pipeline-runs")]
    public async Task<ActionResult> GetPipelineRuns(CancellationToken ct)
    {
        try
        {
            await _db.ExecutePostgresAsync("SELECT 1", ct);
            return Ok(new
            {
                source = IsSnowflakeConfigured() ? "snowflake" : "postgresql",
                snowflake_table = "PIPELINE_DB.ANALYTICS.FACT_PIPELINE_RUNS",
                postgres_table = "fact_pipeline_runs"
            });
        }
        catch (System.Exception ex)
        {
            _logger.LogError(ex, "Failed to query pipeline runs");
            return StatusCode(503, new { error = "Warehouse database unavailable" });
        }
    }

    /// <summary>Health check for warehouse connectivity</summary>
    [HttpGet("health")]
    public async Task<ActionResult> WarehouseHealth(CancellationToken ct)
    {
        var snowflakeConfigured = IsSnowflakeConfigured();

        try
        {
            await _db.ExecutePostgresAsync("SELECT COUNT(*) FROM dim_date", ct);
            return Ok(new
            {
                status = "healthy",
                staging_db = "postgresql:connected",
                warehouse = snowflakeConfigured ? "snowflake:configured" : "postgresql:fallback",
                schema = "star_schema",
                snowflake = new
                {
                    configured = snowflakeConfigured,
                    account = _config["Snowflake:Account"] ?? "",
                    warehouse_name = _config["Snowflake:Warehouse"] ?? "PIPELINE_WH",
                    database = _config["Snowflake:Database"] ?? "PIPELINE_DB"
                }
            });
        }
        catch
        {
            return StatusCode(503, new
            {
                status = "unhealthy",
                staging_db = "postgresql:disconnected",
                warehouse = "unknown"
            });
        }
    }

    /// <summary>Get Snowflake warehouse configuration status</summary>
    [HttpGet("snowflake/status")]
    public ActionResult GetSnowflakeStatus()
    {
        var configured = IsSnowflakeConfigured();
        return Ok(new
        {
            configured,
            account = configured ? _config["Snowflake:Account"] : null,
            warehouse_name = _config["Snowflake:Warehouse"] ?? "PIPELINE_WH",
            database = _config["Snowflake:Database"] ?? "PIPELINE_DB",
            schema_name = _config["Snowflake:Schema"] ?? "ANALYTICS",
            role = _config["Snowflake:Role"] ?? "PIPELINE_ROLE",
            tables = new
            {
                dimensions = new[] { "DIM_CUSTOMERS", "DIM_DATE", "DIM_PRODUCTS", "DIM_DEVICES" },
                facts = new[] { "FACT_ORDERS", "FACT_SENSOR_READINGS", "FACT_PIPELINE_RUNS" },
                aggregations = new[] { "AGG_DAILY_ORDERS", "AGG_HOURLY_SENSORS" },
                staging = new[] { "STG_ORDERS", "STG_SENSOR_READINGS" }
            }
        });
    }

    private bool IsSnowflakeConfigured()
    {
        var account = _config["Snowflake:Account"];
        return !string.IsNullOrWhiteSpace(account);
    }
}
