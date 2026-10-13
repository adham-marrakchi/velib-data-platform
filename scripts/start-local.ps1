# =============================================================
# Velib' Data Platform - demarrage local sous Windows (PowerShell)
# =============================================================
# A lancer depuis la racine du depot :
#   powershell -ExecutionPolicy Bypass -File scripts\start-local.ps1
#
# Etapes : verifie Docker, met le code a jour (git pull), complete .env (cle OpenRouter),
# construit et lance la plateforme, remplit les donnees (stations, Silver, Gold),
# puis ouvre le chatbot sur http://localhost:8090.
#
# Options :
#   -SkipPull   ne pas faire git pull
#   -SkipInit   ne pas relancer le remplissage des donnees (deja fait)
#   -Rebuild    forcer la reconstruction des images Docker

param(
    [switch]$SkipPull,
    [switch]$SkipInit,
    [switch]$Rebuild
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

function Step($msg) { Write-Host "`n==> $msg" -ForegroundColor Cyan }
function Ok($msg) { Write-Host "    OK  $msg" -ForegroundColor Green }
function Warn($msg) { Write-Host "    !!  $msg" -ForegroundColor Yellow }
function Fail($msg) { Write-Host "`nERREUR : $msg" -ForegroundColor Red; exit 1 }

# Execute une commande externe et echoue proprement si elle renvoie un code non nul.
function Run([string]$exe, [string[]]$arguments) {
    & $exe @arguments
    if ($LASTEXITCODE -ne 0) { Fail "la commande '$exe $($arguments -join ' ')' a echoue (code $LASTEXITCODE)." }
}

# Lit les cles d'un fichier .env (lignes CLE=valeur, commentaires ignores).
function Read-EnvKeys([string]$path) {
    $keys = [ordered]@{}
    foreach ($line in [IO.File]::ReadAllLines($path)) {
        if ($line -match '^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)$') { $keys[$Matches[1]] = $Matches[2] }
    }
    return $keys
}

# Ecrit en UTF-8 sans BOM avec des fins de ligne LF (format attendu par docker compose).
function Write-Lf([string]$path, [string[]]$lines) {
    $utf8 = New-Object System.Text.UTF8Encoding($false)
    [IO.File]::WriteAllText($path, (($lines -join "`n") + "`n"), $utf8)
}

# ---------------------------------------------------------------------------
Step "Verification des prerequis"
if (-not (Get-Command git -ErrorAction SilentlyContinue)) { Fail "git est introuvable. Installe Git for Windows : https://git-scm.com/download/win" }
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { Fail "docker est introuvable. Installe Docker Desktop : https://www.docker.com/products/docker-desktop/" }
docker info *> $null
if ($LASTEXITCODE -ne 0) { Fail "Docker Desktop n'est pas demarre. Lance-le, attends qu'il soit pret, puis relance ce script." }
Ok "git et Docker Desktop sont prets"

$mem = docker info --format "{{.MemTotal}}" 2>$null
if ($mem -and [int64]$mem -lt 7GB) {
    Warn ("Docker n'a que {0:N1} Go de RAM. Il en faut 8 : Docker Desktop > Settings > Resources > Memory." -f ($mem / 1GB))
}
$drive = Get-PSDrive -Name ((Get-Location).Path.Substring(0, 1))
if ($drive.Free -lt 25GB) {
    Warn ("Seulement {0:N0} Go libres sur {1}:. Le premier build en demande environ 25." -f ($drive.Free / 1GB), $drive.Name)
}

# ---------------------------------------------------------------------------
if (-not $SkipPull) {
    Step "Mise a jour du code (git pull)"
    $dirty = git status --porcelain --untracked-files=no
    if ($dirty) {
        Warn "Tu as des fichiers modifies localement : je ne fais pas de git pull pour ne rien ecraser."
        Warn "Fais 'git stash' ou commite-les, puis relance (ou utilise -SkipPull)."
        $dirty | ForEach-Object { Write-Host "       $_" }
        Fail "mise a jour annulee."
    }
    Run git @("fetch", "origin")
    Run git @("checkout", "main")
    Run git @("pull", "--ff-only", "origin", "main")
    Ok "code a jour : $(git log --oneline -1)"
}

# ---------------------------------------------------------------------------
Step "Configuration (.env)"
if (-not (Test-Path ".env.example")) { Fail "fichier .env.example introuvable : lance le script depuis le dossier du projet." }
$example = Read-EnvKeys (Join-Path $Root ".env.example")
if (-not (Test-Path ".env")) {
    Copy-Item (Join-Path $Root ".env.example") (Join-Path $Root ".env")
    Ok ".env cree a partir de .env.example"
}
$envLines = [System.Collections.Generic.List[string]]::new()
$envLines.AddRange([string[]][IO.File]::ReadAllLines((Join-Path $Root ".env")))
$current = Read-EnvKeys (Join-Path $Root ".env")

# Ajoute les variables apparues dans .env.example depuis la creation de ton .env
$added = @()
foreach ($k in $example.Keys) {
    if (-not $current.Contains($k)) { $envLines.Add("$k=$($example[$k])"); $current[$k] = $example[$k]; $added += $k }
}
if ($added.Count) { Ok ("variables ajoutees a .env : " + ($added -join ", ")) }

# Cle OpenRouter : demandee si absente, jamais affichee
if (-not $current["OPENROUTER_API_KEY"]) {
    Write-Host "    Colle ta cle OpenRouter (https://openrouter.ai/settings/keys), puis Entree :"
    $secure = Read-Host -AsSecureString
    $key = [Runtime.InteropServices.Marshal]::PtrToStringBSTR([Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)).Trim()
    if (-not $key.StartsWith("sk-or-")) { Fail "cette cle ne ressemble pas a une cle OpenRouter (elle doit commencer par sk-or-)." }
    for ($i = 0; $i -lt $envLines.Count; $i++) {
        if ($envLines[$i] -match '^\s*OPENROUTER_API_KEY=') { $envLines[$i] = "OPENROUTER_API_KEY=$key" }
    }
    Ok "cle OpenRouter enregistree dans .env (fichier ignore par git)"
}
else { Ok "cle OpenRouter deja presente dans .env" }
Write-Lf (Join-Path $Root ".env") $envLines

# ---------------------------------------------------------------------------
Step "Construction et demarrage de la plateforme (le premier build prend 15 a 30 min)"
$upArgs = @("compose", "up", "-d")
if ($Rebuild) { $upArgs += "--build" }
else {
    $missing = @("velib/airflow:dev", "velib/spark:dev", "velib/producer:dev", "velib/mlflow:dev", "velib/assistant:dev") |
        Where-Object { -not (docker images -q $_) }
    if ($missing) { $upArgs += "--build" }
}
Run docker $upArgs

Step "Attente des services (Airflow, Kafka, PostgreSQL...)"
$deadline = (Get-Date).AddMinutes(10)
while ($true) {
    $states = docker compose ps --format "{{.Name}} {{.Status}}"
    $starting = @($states | Where-Object { $_ -match "health: starting|Restarting" })
    $airflowOk = $states | Where-Object { $_ -match "^airflow-scheduler .*\(healthy\)" }
    if ($airflowOk -and $starting.Count -eq 0) { break }
    if ((Get-Date) -gt $deadline) {
        $states | ForEach-Object { Write-Host "       $_" }
        Fail "les services ne sont pas prets apres 10 min. Regarde 'docker compose logs <service>'."
    }
    Start-Sleep -Seconds 10
}
$unhealthy = docker compose ps --format "{{.Name}} {{.Status}}" | Where-Object { $_ -match "unhealthy" }
if ($unhealthy) { $unhealthy | ForEach-Object { Warn "service en difficulte : $_" } }
Ok "services demarres"

# ---------------------------------------------------------------------------
if (-not $SkipInit) {
    Step "Remplissage des donnees (environ 10 min : stations + geocodage, Silver, Gold)"
    Write-Host "    Laisse 2 min au producteur et a Spark pour remplir la couche Bronze..."
    Start-Sleep -Seconds 120
    foreach ($dag in @("velib_stations_reference_dag", "velib_silver_quality_dag", "dbt_gold_dag")) {
        Write-Host "    - $dag"
        docker compose exec -T airflow-scheduler airflow dags test $dag *> "$env:TEMP\velib_$dag.log"
        if ($LASTEXITCODE -ne 0 -or -not (Select-String -Path "$env:TEMP\velib_$dag.log" -Pattern "DagRun Finished.*[ ,]state=success" -Quiet)) {
            Fail "le DAG $dag a echoue. Journal : $env:TEMP\velib_$dag.log"
        }
        Ok "$dag termine"
    }
}

# ---------------------------------------------------------------------------
Step "Verification du chatbot"
$health = $null
for ($i = 0; $i -lt 12; $i++) {
    try { $health = Invoke-RestMethod -Uri "http://localhost:8090/api/health" -TimeoutSec 5; break } catch { Start-Sleep -Seconds 5 }
}
if (-not $health) { Fail "le chatbot ne repond pas sur http://localhost:8090. Regarde 'docker compose logs assistant'." }
if (-not $health.database) { Fail "le chatbot ne joint pas PostgreSQL. Regarde 'docker compose logs assistant postgres'." }
if (-not $health.api_key_configured) { Fail "OPENROUTER_API_KEY est vide dans .env. Remplis-la puis : docker compose up -d assistant" }
if (-not $health.last_snapshot) { Warn "pas encore de donnees Gold : relance le script sans -SkipInit." }
else { Ok "donnees Gold disponibles (dernier releve : $($health.last_snapshot))" }

Write-Host ""
Write-Host "Tout est pret." -ForegroundColor Green
Write-Host "  Chatbot     http://localhost:8090"
Write-Host "  Airflow     http://localhost:8080   (admin / admin_2026)"
Write-Host "  MinIO       http://localhost:9001   (minio / minio_secret_2026)"
Write-Host "  MLflow      http://localhost:5001"
Write-Host "Arreter : docker compose down    |    Relancer plus tard : docker compose up -d"
try { Start-Process "http://localhost:8090" } catch { Write-Host "Ouvre http://localhost:8090 dans ton navigateur." }
