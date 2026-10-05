FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["src/DataPipelineApi/DataPipelineApi.csproj", "DataPipelineApi/"]
RUN dotnet restore "DataPipelineApi/DataPipelineApi.csproj"
COPY src/DataPipelineApi/. ./DataPipelineApi
COPY appsettings.json DataPipelineApi/appsettings.json
COPY appsettings.Production.json DataPipelineApi/appsettings.Production.json
WORKDIR /src/DataPipelineApi
RUN dotnet publish -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app

# Install curl for healthchecks
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/publish .

ENV ASPNETCORE_URLS=http://+:80
ENV ASPNETCORE_ENVIRONMENT=Production
EXPOSE 80

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:80/health/live || exit 1

ENTRYPOINT ["dotnet", "DataPipelineApi.dll"]
