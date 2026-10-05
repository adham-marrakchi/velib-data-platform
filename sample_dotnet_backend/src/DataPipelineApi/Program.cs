using System.Text.Json;
using DataPipelineApi.HealthChecks;
using DataPipelineApi.Options;
using DataPipelineApi.Services;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.HttpLogging;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.OpenApi.Models;
using Polly;
using Polly.Extensions.Http;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .CreateBootstrapLogger();

try
{
    var builder = WebApplication.CreateBuilder(args);

    // Serilog
    builder.Host.UseSerilog((ctx, lc) => lc.ReadFrom.Configuration(ctx.Configuration));

    // Options binding with validation
    builder.Services.AddOptions<DatabaseOptions>().Bind(builder.Configuration.GetSection("ConnectionStrings")).ValidateDataAnnotations().ValidateOnStart();
    builder.Services.AddOptions<MinioOptions>().Bind(builder.Configuration.GetSection("Minio")).ValidateDataAnnotations().ValidateOnStart();
    builder.Services.AddOptions<KafkaOptions>().Bind(builder.Configuration.GetSection("Kafka")).ValidateDataAnnotations().ValidateOnStart();
    builder.Services.AddOptions<AirflowOptions>().Bind(builder.Configuration.GetSection("Airflow")).ValidateDataAnnotations().ValidateOnStart();
    builder.Services.AddOptions<GEOptions>().Bind(builder.Configuration.GetSection("GreatExpectations")).ValidateDataAnnotations().ValidateOnStart();
    builder.Services.AddOptions<AtlasOptions>().Bind(builder.Configuration.GetSection("Atlas")).ValidateDataAnnotations().ValidateOnStart();
    builder.Services.AddOptions<MLflowOptions>().Bind(builder.Configuration.GetSection("MLflow")).ValidateDataAnnotations().ValidateOnStart();
    builder.Services.AddOptions<GitHubOptions>().Bind(builder.Configuration.GetSection("GitHub")).ValidateDataAnnotations().ValidateOnStart();

    // Retry policy
    var retryPolicy = HttpPolicyExtensions.HandleTransientHttpError()
        .WaitAndRetryAsync(3, retry => TimeSpan.FromSeconds(Math.Pow(2, retry)));

    // HTTP clients with retry
    builder.Services.AddHttpClient<IBatchService, BatchService>((sp, http) =>
    {
        var opt = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<AirflowOptions>>().Value;
        http.BaseAddress = new Uri(opt.BaseUrl);
        http.Timeout = TimeSpan.FromSeconds(opt.RequestTimeoutSeconds);
        http.DefaultRequestHeaders.UserAgent.ParseAdd("DataPipelineApi/2.0");
    }).AddPolicyHandler(retryPolicy);

    builder.Services.AddHttpClient<IStreamingService, StreamingService>((sp, http) =>
    {
        var opt = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<AirflowOptions>>().Value;
        http.BaseAddress = new Uri(opt.BaseUrl);
        http.Timeout = TimeSpan.FromSeconds(opt.RequestTimeoutSeconds);
        http.DefaultRequestHeaders.UserAgent.ParseAdd("DataPipelineApi/2.0");
    }).AddPolicyHandler(retryPolicy);

    builder.Services.AddHttpClient<IAtlasService, AtlasService>((sp, http) =>
    {
        var opt = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<AtlasOptions>>().Value;
        http.BaseAddress = new Uri(opt.Endpoint);
        http.Timeout = TimeSpan.FromSeconds(30);
    }).AddPolicyHandler(retryPolicy);

    builder.Services.AddHttpClient<IMLflowService, MLflowService>((sp, http) =>
    {
        var opt = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<MLflowOptions>>().Value;
        http.BaseAddress = new Uri(opt.TrackingUri);
        http.Timeout = TimeSpan.FromSeconds(opt.RequestTimeoutSeconds);
    }).AddPolicyHandler(retryPolicy);

    builder.Services.AddHttpClient<ICIService, CIService>((sp, http) =>
    {
        var opt = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<GitHubOptions>>().Value;
        http.Timeout = TimeSpan.FromSeconds(30);
        http.DefaultRequestHeaders.UserAgent.ParseAdd(opt.UserAgent);
    });

    builder.Services.AddHttpClient("airflow-health", (sp, http) =>
    {
        var opt = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<AirflowOptions>>().Value;
        http.BaseAddress = new Uri(opt.BaseUrl);
        http.Timeout = TimeSpan.FromSeconds(opt.RequestTimeoutSeconds);
        var tok = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{opt.Username}:{opt.Password}"));
        http.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", tok);
    });
    builder.Services.AddHttpClient("mlflow-health", (sp, http) =>
    {
        var opt = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<MLflowOptions>>().Value;
        http.BaseAddress = new Uri(opt.TrackingUri);
        http.Timeout = TimeSpan.FromSeconds(opt.RequestTimeoutSeconds);
    });

    // Services
    builder.Services.AddSingleton<IDbService, DbService>();
    builder.Services.AddSingleton<IStorageService, MinioService>();
    builder.Services.AddSingleton<IKafkaService, KafkaService>();
    builder.Services.AddSingleton<IGEValidationService, GEValidationService>();
    builder.Services.AddSingleton<IMonitoringService, MonitoringService>();

    // Health checks
    builder.Services.AddHealthChecks()
        .AddCheck<MySqlHealthCheck>("mysql", tags: new[] { "db", "critical" })
        .AddCheck<PostgresHealthCheck>("postgres", tags: new[] { "db", "critical" })
        .AddCheck<MinioHealthCheck>("minio", tags: new[] { "storage", "critical" })
        .AddCheck<KafkaHealthCheck>("kafka", tags: new[] { "messaging", "critical" })
        .AddCheck<AirflowHealthCheck>("airflow", tags: new[] { "orchestration" })
        .AddCheck<MLflowHealthCheck>("mlflow", tags: new[] { "ml" });

    // CORS
    builder.Services.AddCors(options =>
    {
        options.AddDefaultPolicy(policy =>
        {
            policy.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader();
        });
    });

    builder.Services.AddHttpLogging(logging =>
    {
        logging.LoggingFields = HttpLoggingFields.RequestMethod | HttpLoggingFields.RequestPath | HttpLoggingFields.ResponseStatusCode | HttpLoggingFields.Duration;
    });

    builder.Services.AddResponseCompression();

    builder.Services.AddControllers().AddNewtonsoftJson()
        .ConfigureApiBehaviorOptions(options =>
        {
            options.InvalidModelStateResponseFactory = context =>
            {
                var details = new ValidationProblemDetails(context.ModelState)
                {
                    Status = StatusCodes.Status400BadRequest,
                    Title = "Invalid request payload"
                };
                return new BadRequestObjectResult(details);
            };
        });

    builder.Services.AddEndpointsApiExplorer();
    builder.Services.AddSwaggerGen(c =>
    {
        c.SwaggerDoc("v1", new OpenApiInfo
        {
            Title = "E2E Data Pipeline API",
            Version = "v2.0",
            Description = "Enterprise-grade API for orchestrating batch ingestion, streaming, data governance, ML pipelines, and warehouse operations.",
            Contact = new OpenApiContact { Name = "Data Engineering Team" }
        });
    });

    var app = builder.Build();

    app.UseSerilogRequestLogging();

    app.UseForwardedHeaders(new ForwardedHeadersOptions
    {
        ForwardedHeaders = ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto
    });

    app.UseExceptionHandler(errorApp =>
    {
        errorApp.Run(async context =>
        {
            var problem = new ProblemDetails
            {
                Status = StatusCodes.Status500InternalServerError,
                Title = "Internal server error",
                Instance = context.Request.Path
            };
            problem.Extensions["traceId"] = context.TraceIdentifier;
            context.Response.StatusCode = problem.Status.Value;
            context.Response.ContentType = "application/problem+json";
            await context.Response.WriteAsJsonAsync(problem);
        });
    });

    // Swagger available in all environments
    app.UseSwagger();
    app.UseSwaggerUI(c =>
    {
        c.SwaggerEndpoint("/swagger/v1/swagger.json", "E2E Data Pipeline API v2.0");
        c.RoutePrefix = "swagger";
    });

    app.UseCors();
    app.UseHttpLogging();
    app.UseResponseCompression();

    // Request ID middleware
    app.Use(async (context, next) =>
    {
        var requestId = context.Request.Headers["X-Request-ID"].FirstOrDefault() ?? Guid.NewGuid().ToString();
        context.Response.Headers["X-Request-ID"] = requestId;
        using (Serilog.Context.LogContext.PushProperty("RequestId", requestId))
        {
            await next();
        }
    });

    app.UseRouting();
    app.MapControllers();

    // Health endpoints
    app.MapHealthChecks("/health", new HealthCheckOptions
    {
        ResponseWriter = async (ctx, report) =>
        {
            ctx.Response.ContentType = "application/json";
            var payload = new
            {
                status = report.Status.ToString(),
                totalDuration = report.TotalDuration.TotalMilliseconds + "ms",
                results = report.Entries.Select(e => new
                {
                    key = e.Key,
                    status = e.Value.Status.ToString(),
                    duration = e.Value.Duration.TotalMilliseconds + "ms",
                    description = e.Value.Description,
                    tags = e.Value.Tags
                })
            };
            await ctx.Response.WriteAsync(JsonSerializer.Serialize(payload));
        }
    });

    app.MapHealthChecks("/health/ready", new HealthCheckOptions
    {
        Predicate = check => check.Tags.Contains("critical"),
        ResponseWriter = async (ctx, report) =>
        {
            ctx.Response.ContentType = "application/json";
            await ctx.Response.WriteAsync(JsonSerializer.Serialize(new { status = report.Status.ToString() }));
        }
    });

    app.MapHealthChecks("/health/live", new HealthCheckOptions
    {
        Predicate = _ => false,
        ResponseWriter = async (ctx, _) =>
        {
            ctx.Response.ContentType = "application/json";
            await ctx.Response.WriteAsync(JsonSerializer.Serialize(new { status = "Healthy" }));
        }
    });

    Log.Information("Data Pipeline API starting on {Urls}", builder.Configuration["ASPNETCORE_URLS"] ?? "http://+:80");
    app.Run();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Application terminated unexpectedly");
}
finally
{
    Log.CloseAndFlush();
}
