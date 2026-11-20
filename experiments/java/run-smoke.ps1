# Replayable Java baseline smoke for Cipple++ experiments (third wave, portable).
# Only writes: experiments/java/results/<ts>/ and %TEMP%\opencode\ripple-smoke-*.
# Never touches Ripple working tree (read-only git archive of pinned SHA). Never commits.
# Stop only processes started by this script. No Remove-Item -Force anywhere.
param(
  [string]$RippleDir = '',
  [string]$RipplePinnedSha = '468e9cb4296110467e3cfdafed412b3dbecfd52a',
  [string]$JavaExe = 'java',
  [string]$JavacExe = 'javac',
  [string]$MavenCmd = 'mvn',
  [string]$ExperimentsDir = '',
  [string]$JavaBuildReuse = '',
  [int]$ServerApiPort = 13201,
  [int]$ServerUiPort = 14201,
  [int]$ClientApiPort = 15201,
  [int]$ClientUiPort = 16201,
  [string]$App = 'smokeapp',
  [string]$Key = 'smokekey'
)
$ErrorActionPreference = 'Stop'

if ([string]::IsNullOrWhiteSpace($ExperimentsDir)) {
  $ExperimentsDir = Split-Path $PSScriptRoot -Parent
}
$RepoRoot = Split-Path $ExperimentsDir -Parent
if ([string]::IsNullOrWhiteSpace($RippleDir)) {
  $RippleDir = Join-Path (Split-Path $RepoRoot -Parent) 'Ripple'
}

function Resolve-Exe($name, $fallbackList) {
  try {
    $c = Get-Command $name -ErrorAction Stop
    if ($c.Source) { return $c.Source }
  } catch {}
  foreach ($fb in $fallbackList) {
    if ($fb -and (Test-Path $fb)) { return $fb }
    try {
      $c2 = Get-Command $fb -ErrorAction Stop
      if ($c2.Source) { return $c2.Source }
    } catch {}
  }
  return $name
}
$MavenResolved = Resolve-Exe $MavenCmd @('mvn.cmd', (Join-Path $env:TEMP 'opencode\apache-maven-3.9.9\bin\mvn.cmd'))
$JavaResolved = Resolve-Exe $JavaExe @()
$JavacResolved = Resolve-Exe $JavacExe @()

$ts = Get-Date -Format 'yyyy-MM-dd-HHmmss'
$stamp = "smoke-$ts"
$work = Join-Path $env:TEMP "opencode\ripple-smoke-$stamp"
$build = Join-Path $work 'build'
$run = Join-Path $work 'run'
$serverHome = Join-Path $run 'server1'
$clientHome = Join-Path $run 'client1'
$driverHome = Join-Path $run 'driver'
$results = Join-Path $ExperimentsDir "java\results\$stamp"
New-Item -ItemType Directory -Force -Path $build, $run, $serverHome, $clientHome, $driverHome, $results | Out-Null
$log = Join-Path $results 'orchestrator.log'
function Note($s) { "[$(Get-Date -Format 'HH:mm:ss')] $s" | Tee-Object $log -Append | Write-Host }

$serverProc = $null; $clientProc = $null
function Stop-Started {
  foreach ($process in @($clientProc, $serverProc)) {
    if ($process -and -not $process.HasExited) {
      try { $process.Kill(); $process.WaitForExit(5000) | Out-Null } catch {}
    }
  }
}
trap { Note "TRAP: $_"; Stop-Started; break }

try {
Note "PHASE 0: resolve (experiments=$ExperimentsDir ripple=$RippleDir java=$JavaResolved mvn=$MavenResolved)"
$rippleHead = (& git -C $RippleDir rev-parse HEAD).Trim()
Note "ripple HEAD(worktree)=$rippleHead pinned=$RipplePinnedSha"
& git -C $RippleDir cat-file -e "$RipplePinnedSha^{commit}" 2>&1
if ($LASTEXITCODE -ne 0) { throw "pinned Ripple SHA not found: $RipplePinnedSha" }
if ($rippleHead -ne $RipplePinnedSha) { Note "WARNING: worktree HEAD moved; build uses pinned $RipplePinnedSha" }
"ripple_head=$rippleHead`nripple_pinned=$RipplePinnedSha" | Out-File (Join-Path $results 'baseline.txt')
"ripple status (must not be modified by this script):" | Out-File (Join-Path $results 'baseline.txt') -Append
git -C $RippleDir status --short --branch | Out-File (Join-Path $results 'baseline.txt') -Append
try { & $JavaResolved -version 2>&1 | Out-File (Join-Path $results 'java-version.txt') } catch { "java failed: $_" | Out-File (Join-Path $results 'java-version.txt') }

Note 'PHASE 1: Java baseline build (pinned archive; reuse verified path when valid)'
$useBuild = $null
$reuseMarker = if ($JavaBuildReuse) { Join-Path $JavaBuildReuse '.reuse-pin.txt' }
if ($reuseMarker -and (Test-Path $reuseMarker) -and
    ((Get-Content -LiteralPath $reuseMarker -TotalCount 1) -split '\s+')[0] -eq $RipplePinnedSha -and
    (Test-Path (Join-Path $JavaBuildReuse 'ripple-server\target\ripple-server-publish.tar.gz')) -and
    (Test-Path (Join-Path $JavaBuildReuse 'ripple-client\target\ripple-client-publish.tar.gz')) -and
    (Test-Path (Join-Path $JavaBuildReuse 'ripple-client\target\classes'))) {
  $useBuild = $JavaBuildReuse
  Note "reuse verified Java build at $useBuild"
} else {
  Note "building from git archive $RipplePinnedSha ..."
  $archive = Join-Path $work 'ripple-pinned.tar'
  & git -C $RippleDir archive --format=tar -o $archive $RipplePinnedSha
  if ($LASTEXITCODE -ne 0) { throw 'git archive pinned failed' }
  tar -xf $archive -C $build
  & $MavenResolved -f (Join-Path $build 'pom.xml') install -DskipTests 2>&1 | Tee-Object (Join-Path $results 'mvn-install.log') | Select-Object -Last 8
  if ($LASTEXITCODE -ne 0) { throw 'mvn install failed' }
  $useBuild = $build
  $RipplePinnedSha | Out-File (Join-Path $useBuild '.reuse-pin.txt') -Encoding utf8
  Note "fresh build OK -> $useBuild"
}
"java_build_source=$useBuild" | Out-File (Join-Path $results 'baseline.txt') -Append

Note 'PHASE 2: start server + client (sequential setup)'
tar -xzf (Join-Path $useBuild 'ripple-server\target\ripple-server-publish.tar.gz') -C $serverHome
tar -xzf (Join-Path $useBuild 'ripple-client\target\ripple-client-publish.tar.gz') -C $clientHome
$serverLog = Join-Path $results 'server1.log'
$clientLog = Join-Path $results 'client1.log'
$nodes = "1:127.0.0.1:$ServerApiPort"
$serverArgs = @("-Did=1", '-Daddress=127.0.0.1', "-DapiPort=$ServerApiPort", "-DuiPort=$ServerUiPort",
  ('"-DstorageLocation={0}"' -f (Join-Path $serverHome 'node-1.db')), "-Dnodes=$nodes", '-Dprotocol=star',
  '-jar', ('"{0}"' -f (Join-Path $serverHome 'ripple-server.jar')))
$clientArgs = @('-Daddress=127.0.0.1', "-DapiPort=$ClientApiPort", "-DuiPort=$ClientUiPort",
  ('"-DstorageLocation={0}"' -f (Join-Path $clientHome 'client-1.db')), "-Dnodes=$nodes", '-DnodeSelector=hashing',
  '-jar', ('"{0}"' -f (Join-Path $clientHome 'ripple-client.jar')))
$serverProc = Start-Process $JavaResolved -WindowStyle Hidden -ArgumentList $serverArgs -RedirectStandardOutput $serverLog -RedirectStandardError ($serverLog + '.stderr') -PassThru
$clientProc = Start-Process $JavaResolved -WindowStyle Hidden -ArgumentList $clientArgs -RedirectStandardOutput $clientLog -RedirectStandardError ($clientLog + '.stderr') -PassThru
"server pid=$($serverProc.Id) client pid=$($clientProc.Id)`njava_build=$useBuild`npinned=$RipplePinnedSha" | Out-File (Join-Path $results 'pids.txt')
function Wait-Port($port, $timeoutSec = 60) {
  $end = (Get-Date).AddSeconds($timeoutSec)
  while ((Get-Date) -lt $end) {
    if (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue) { return $true }
    Start-Sleep -Milliseconds 500
  }
  return $false
}
if (-not (Wait-Port $ServerApiPort)) { throw "server api port $ServerApiPort never listened; see $serverLog" }
if (-not (Wait-Port $ClientApiPort)) { throw "client api port $ClientApiPort never listened; see $clientLog" }
Start-Sleep -Seconds 3
Note 'PHASE 2 END: listening'

Note 'PHASE 3: compile + run Driver (measured)'
$libs = (Get-ChildItem (Join-Path $serverHome 'lib\*.jar') | ForEach-Object { $_.FullName }) -join ';'
$cpBuild = "$libs;$useBuild\ripple-client\target\classes;$useBuild\ripple-common\target\classes"
Copy-Item (Join-Path $ExperimentsDir 'java\Driver.java') (Join-Path $driverHome 'Driver.java')
& $JavacResolved -encoding UTF-8 -cp $cpBuild -d $driverHome (Join-Path $driverHome 'Driver.java')
if ($LASTEXITCODE -ne 0) { throw 'javac Driver failed' }
$resultJson = Join-Path $run 'smoke-result.json'
$dbDir = Join-Path $run 'driverdb'
$cpRun = "$driverHome;$libs;$useBuild\ripple-client\target\classes;$useBuild\ripple-common\target\classes"
& $JavaResolved -cp $cpRun '-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN' Driver $nodes $App $Key $dbDir $resultJson 2>&1 | Tee-Object (Join-Path $results 'driver.log')
$driverExit = $LASTEXITCODE
Copy-Item $resultJson (Join-Path $results 'smoke-result.json')
Note "PHASE 3 END: driver exit=$driverExit"
"driver exit = $driverExit" | Out-File (Join-Path $results 'summary.txt')
Get-Content $resultJson | Out-File (Join-Path $results 'summary.txt') -Append
if ($driverExit -ne 0) { throw "smoke driver failed (exit $driverExit); see $results" }
Write-Host "SMOKE OK -> $results"
} finally {
  Note 'cleanup: stopping only started processes'
  Stop-Started
}
