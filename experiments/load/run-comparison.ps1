# Same-workload Java vs Cipple++ load comparison (third wave, portable).
# Only writes: experiments/load/results/<stamp>/ and %TEMP%\opencode\cipple-load-*.
# Never touches Ripple working tree (read-only git archive of pinned SHA) and never commits.
# Only stops processes started by this script. No Remove-Item -Force anywhere.
param(
  [string]$RippleDir = '',
  [string]$RipplePinnedSha = '468e9cb4296110467e3cfdafed412b3dbecfd52a',
  [string]$JavaExe = 'java',
  [string]$JavacExe = 'javac',
  [string]$MavenCmd = 'mvn',
  [string]$PythonExe = 'python',
  [string]$MsysBin = '',
  [string]$ExperimentsDir = '',
  [string]$CippleServerExe = '',
  [string]$CippleCodeRevision = '',
  [string]$JavaBuildReuse = '',
  [string]$OutputRoot = '',
  [int]$Servers = 1,
  [int]$Subscribers = 1,
  [int]$Warmup = 5,
  [int]$Ops = 20,
  [int]$GapMs = 50,
  [int]$DeadlineMs = 15000,
  [int]$ValueBytes = 256,
  [string]$JavaProtocol = 'star',
  [string]$JavaLogLevel = 'WARN',
  [int]$JavaEventLoopThreads = 0,
  [int]$JavaDriverTimeoutSeconds = 180,
  [string]$CppOverlay = 'star',
  [int]$JavaApiBase = 13301,
  [int]$JavaUiBase = 14301,
  [int]$CppBase = 13401,
  [string]$App = 'benchapp',
  [string]$Key = 'benchkey',
  [switch]$ProbeDurable,
  [switch]$MeasurePersist,
  [switch]$SkipJavaBuild
)
$ErrorActionPreference = 'Stop'

# ---- Portable path resolution (no hardcoded user/profile dirs) ----
if ([string]::IsNullOrWhiteSpace($ExperimentsDir)) {
  $ExperimentsDir = Split-Path $PSScriptRoot -Parent
}
$RepoRoot = Split-Path $ExperimentsDir -Parent
if ([string]::IsNullOrWhiteSpace($RippleDir)) {
  $RippleDir = Join-Path (Split-Path $RepoRoot -Parent) 'Ripple'
}
if ([string]::IsNullOrWhiteSpace($CippleServerExe)) {
  if ($env:CIPPLE_SERVER_EXE -and (Test-Path $env:CIPPLE_SERVER_EXE)) {
    $CippleServerExe = $env:CIPPLE_SERVER_EXE
  } else {
    $CippleServerExe = Join-Path $RepoRoot 'build\server\cipple_server.exe'
  }
}
if (-not $MsysBin -and $env:UCRT64_ROOT) {
  $MsysBin = Join-Path $env:UCRT64_ROOT bin
}
if (-not $OutputRoot) {
  $OutputRoot = Join-Path $ExperimentsDir 'load\results'
}

function Resolve-Exe($name, $fallbackList) {
  foreach ($cand in @($name) + $fallbackList) {
    if ([string]::IsNullOrWhiteSpace($cand)) { continue }
    $path = $null
    if (Test-Path $cand) { $path = $cand }
    else {
      try {
        $c = Get-Command $cand -ErrorAction Stop
        if ($c.Source) { $path = $c.Source }
      } catch {}
    }
    if (-not $path) { continue }
    # Reject WindowsApps python stub (0-byte / Store redirect): must run --version.
    try {
      $old = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
      & $path --version 2>&1 | Out-Null
      $ok = ($LASTEXITCODE -eq 0)
      $ErrorActionPreference = $old
      if ($ok) { return $path }
    } catch {}
  }
  return $name
}

# Candidate runtime needs ucrt64 DLLs; prepend if present (system tool path, not user dir).
if ($MsysBin -and (Test-Path $MsysBin) -and ($env:Path -notlike "*$MsysBin*")) {
  $env:Path = "$MsysBin;$env:Path"
}

$MavenResolved = Resolve-Exe $MavenCmd @('mvn.cmd', (Join-Path $env:TEMP 'opencode\apache-maven-3.9.9\bin\mvn.cmd'))
# Prefer real interpreters first; bare 'python' is last (avoids WindowsApps stub).
# Explicit -PythonExe (when bound) is honored first.
$pyCands = @('py', 'python3', $PythonExe)
if ($MsysBin) { $pyCands += Join-Path $MsysBin 'python3.exe' }
if ($PSBoundParameters.ContainsKey('PythonExe')) { $pyCands = @($PythonExe) + $pyCands }
$PythonResolved = Resolve-Exe $pyCands[0] @($pyCands[1..($pyCands.Length-1)])
$JavaResolved = Resolve-Exe $JavaExe @()
$JavacResolved = Resolve-Exe $JavacExe @()
if ($PythonResolved -like '*WindowsApps*') {
  throw "Python stub detected ($PythonResolved); install Python or pass -PythonExe <real python> (e.g. msys ucrt64 python3)."
}

$ts = Get-Date -Format 'yyyy-MM-dd-HHmmss'
$stamp = "load-$ts-s${Servers}u${Subscribers}"
$work = Join-Path $env:TEMP "opencode\cipple-load-$stamp"
$build = Join-Path $work 'ripple-build'
$run = Join-Path $work 'run'
$results = Join-Path $OutputRoot $stamp
New-Item -ItemType Directory -Force -Path $build, $run, $results | Out-Null
$log = Join-Path $results 'orchestrator.log'
function Note($s) {
  $line = "[$(Get-Date -Format 'HH:mm:ss')] $s"
  $line | Tee-Object $log -Append | Write-Host
}
function Phase($n, $s) { Note "PHASE $n START: $s" }

# Safety: never leak processes started by this script, even on throw.
$jProcs = @(); $cppProcs = @(); $sampler = $null; $javaDriver = $null
$javaExit = -1; $cppExit = -1; $durExit = -1; $persistExit = -1
function Stop-Children($children) {
  foreach ($p in $children) {
    if (-not $p.HasExited) {
      try { $p.Kill(); $p.WaitForExit(5000) | Out-Null } catch {}
    }
  }
}
function Stop-Started {
  try { if ($sampler) { Stop-Job $sampler -ErrorAction SilentlyContinue } } catch {}
  try { if ($sampler) { Remove-Job $sampler -ErrorAction SilentlyContinue } } catch {}
  Stop-Children $jProcs
  Stop-Children $cppProcs
  if ($javaDriver) { Stop-Children @($javaDriver) }
}
trap { Note "TRAP: $_"; Stop-Started; break }

try {
Phase 0 'resolve + snapshot'
Note "stamp=$stamp work=$work"
Note "experiments=$ExperimentsDir repo=$RepoRoot rippleDir=$RippleDir"
Note "java=$JavaResolved javac=$JavacResolved mvn=$MavenResolved python=$PythonResolved"
Note "config: servers=$Servers subs=$Subscribers warmup=$Warmup ops=$Ops gapMs=$GapMs deadlineMs=$DeadlineMs valueBytes=$ValueBytes javaProto=$JavaProtocol cppOverlay=$CppOverlay probeDurable=$ProbeDurable measurePersist=$MeasurePersist"
Note "cipple_server_exe=$CippleServerExe"
if (-not (Test-Path $CippleServerExe)) { throw "CippleServerExe not found: $CippleServerExe (set -CippleServerExe or `$env:CIPPLE_SERVER_EXE)" }
$cippleHash = (Get-FileHash $CippleServerExe -Algorithm SHA256).Hash
$cippleItem = Get-Item $CippleServerExe
$cippleMtime = $cippleItem.LastWriteTimeUtc.ToString('o')
$cippleSize = $cippleItem.Length
Note "cipple sha256=$cippleHash mtime=$cippleMtime size=$cippleSize"
# Record tested code/ref if available (candidate drop is usually binary-only, no git).
if ($CippleCodeRevision) {
  & git -C $RepoRoot cat-file -e "$CippleCodeRevision^{commit}"
  if ($LASTEXITCODE -ne 0) { throw "CippleCodeRevision is not a commit in $RepoRoot" }
  $cippleRef = (& git -C $RepoRoot rev-parse "$CippleCodeRevision^{commit}").Trim()
} else {
  $cippleRef = 'binary-only-drop(no git ref)'
  try {
  $candDir = Split-Path (Split-Path (Split-Path $CippleServerExe -Parent) -Parent) -Parent
  $gr = & git -C $candDir rev-parse HEAD 2>&1
  if ($LASTEXITCODE -eq 0 -and $gr) { $cippleRef = $gr.Trim() }
  } catch {}
}
Note "cipple ref=$cippleRef"
try { & $CippleServerExe 2>&1 | Select-Object -First 3 | Out-File (Join-Path $results 'cipple-usage.txt') } catch { "usage probe failed: $_" | Out-File (Join-Path $results 'cipple-usage.txt') }
$oldEAP = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
try { & $JavaResolved -version 2>&1 | Out-File (Join-Path $results 'java-version.txt') } catch { "java -version failed: $_" | Out-File (Join-Path $results 'java-version.txt') }
try { & $PythonResolved --version 2>&1 | Out-File (Join-Path $results 'python-version.txt') } catch { "python failed: $_" | Out-File (Join-Path $results 'python-version.txt') }
$ErrorActionPreference = $oldEAP
# Pinned Ripple SHA (full 40-hex) even if worktree HEAD moved.
$rippleHead = (& git -C $RippleDir rev-parse HEAD).Trim()
Note "ripple HEAD(worktree)=$rippleHead pinned=$RipplePinnedSha"
& git -C $RippleDir cat-file -e "$RipplePinnedSha^{commit}" 2>&1
if ($LASTEXITCODE -ne 0) { throw "pinned Ripple SHA not found in $RippleDir : $RipplePinnedSha" }
if ($rippleHead -ne $RipplePinnedSha) { Note "WARNING: worktree HEAD moved; build still uses pinned $RipplePinnedSha" }
git -C $RippleDir status --short --branch | Out-File (Join-Path $results 'java-git-status.txt')
git -C $RepoRoot status --short --branch | Out-File (Join-Path $results 'cpp-git-status.txt')
"ripple_head=$rippleHead`nripple_pinned=$RipplePinnedSha`ncipple_ref=$cippleRef`ncipple_sha256=$cippleHash`ncipple_mtime=$cippleMtime`ncipple_size=$cippleSize`nservers=$Servers subs=$Subscribers warmup=$Warmup ops=$Ops gapMs=$GapMs deadlineMs=$DeadlineMs valueBytes=$ValueBytes javaProto=$JavaProtocol javaLog=$JavaLogLevel cppOverlay=$CppOverlay`ncippleExe=$CippleServerExe`njava=$JavaResolved`nmvn=$MavenResolved`npython=$PythonResolved" | Out-File (Join-Path $results 'config.txt')
"stamp=$stamp`n同一命令复跑见 load/README.md; 候选冻结后由 coordinator 按相同 workload 放大." | Out-File (Join-Path $results 'invocation.txt')
Note "PHASE 0 END"

Phase 1 'Java baseline build (pinned archive; reuse verified path when valid)'
  $reuseOk = $false
  if ($JavaBuildReuse) {
  $reuseMarker = Join-Path $JavaBuildReuse '.reuse-pin.txt'
  if ((Test-Path $reuseMarker) -and ((Get-Content -LiteralPath $reuseMarker -TotalCount 1) -split '\s+')[0] -eq $RipplePinnedSha -and
      (Test-Path (Join-Path $JavaBuildReuse 'ripple-server\target\ripple-server-publish.tar.gz')) -and
      (Test-Path (Join-Path $JavaBuildReuse 'ripple-client\target\classes')) -and
      (Test-Path (Join-Path $JavaBuildReuse 'ripple-common\target\classes'))) {
    $reuseOk = $true
    Note "reuse Java build at $JavaBuildReuse with pin $RipplePinnedSha"
  }
  }
  if ($reuseOk) {
    $useBuild = $JavaBuildReuse
  } else {
    if ($SkipJavaBuild) { throw 'SkipJavaBuild requires a valid JavaBuildReuse from the pinned source.' }
    Note "building Java baseline from git archive $RipplePinnedSha (read-only)..."
    $archive = Join-Path $work 'ripple-pinned.tar'
    & git -C $RippleDir archive --format=tar -o $archive $RipplePinnedSha
    if ($LASTEXITCODE -ne 0) { throw 'git archive pinned SHA failed' }
    tar -xf $archive -C $build
    & $MavenResolved -f (Join-Path $build 'pom.xml') install -DskipTests 2>&1 | Tee-Object (Join-Path $results 'mvn-install.log') -Append | Select-Object -Last 6
    if ($LASTEXITCODE -ne 0) { throw 'mvn install failed; see mvn-install.log' }
    if (-not (Test-Path (Join-Path $build 'ripple-server\target\ripple-server-publish.tar.gz'))) { throw 'missing ripple-server-publish.tar.gz after build' }
    if (-not (Test-Path (Join-Path $build 'ripple-client\target\ripple-client-publish.tar.gz'))) { throw 'missing ripple-client-publish.tar.gz after build' }
    $useBuild = $build
    $RipplePinnedSha | Out-File (Join-Path $useBuild '.reuse-pin.txt') -Encoding utf8
    Note "fresh build OK -> $useBuild"
  }
$serverHome = Join-Path $run 'javahosts'
$jserverPkg = Join-Path $run 'jserver-pkg'
New-Item -ItemType Directory -Force -Path $serverHome, $jserverPkg | Out-Null
tar -xzf (Join-Path $useBuild 'ripple-server\target\ripple-server-publish.tar.gz') -C $jserverPkg
"java_build_source=$useBuild`nreuse=$($useBuild -eq $JavaBuildReuse)" | Out-File (Join-Path $results 'invocation.txt') -Append
$jvmOpts = @()
if ($JavaEventLoopThreads -gt 0) {
  $jvmOpts += "-Dio.netty.eventLoopThreads=$JavaEventLoopThreads"
}
Note "PHASE 1 END: Java package ready ($useBuild), JVM options $jvmOpts"

Phase 2 'Java servers (phase A setup)'
$t0 = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$trace = [ordered]@{
  name = "pinned-$stamp"
  topology = @{ servers = $Servers; subscribers = $Subscribers; protocol_java = $JavaProtocol; overlay_cpp = $CppOverlay }
  payload = @{ app = $App; key = $Key; value_bytes = $ValueBytes }
  offered = @{ mode = 'paced-open-loop'; gap_ms = $GapMs; ops = $Ops; warmup = $Warmup }
  deadlines = @{ per_op_ms = $DeadlineMs; drain_ms = $DeadlineMs }
  placement = 'index-mod: sub i -> nodes[i % N], pub -> nodes[0] (node 1); Java native NodeSelector lambda per sub; Python same'
  pinned = @{ ripple = $RipplePinnedSha; ripple_head = $rippleHead; cipple_sha256 = $cippleHash; cipple_ref = $cippleRef }
  t0_ms = $t0
}
$trace | ConvertTo-Json -Depth 6 | Out-File (Join-Path $results 'trace.json') -Encoding utf8
$jNodes = @()
for ($i = 1; $i -le $Servers; $i++) {
  $api = $JavaApiBase + $i - 1
  $ui = $JavaUiBase + $i - 1
  $jNodes += "${i}:127.0.0.1:${api}"
}
$nodesCsv = $jNodes -join ','
Note "java nodes=$nodesCsv"
$jProcs = @()
for ($i = 1; $i -le $Servers; $i++) {
  $api = $JavaApiBase + $i - 1
  $ui = $JavaUiBase + $i - 1
  $home_ = Join-Path $serverHome "server$i"
  New-Item -ItemType Directory -Force -Path $home_ | Out-Null
  $slog = Join-Path $results "java-server$i.log"
  $p = Start-Process $JavaResolved -WindowStyle Hidden -ArgumentList ($jvmOpts + @("-Did=$i", '-Daddress=127.0.0.1', "-DapiPort=$api", "-DuiPort=$ui", ('"-DstorageLocation={0}"' -f (Join-Path $home_ "node-$i.db")), "-Dnodes=$nodesCsv", "-Dprotocol=$JavaProtocol", "-Dorg.slf4j.simpleLogger.defaultLogLevel=$JavaLogLevel", '-jar', ('"{0}"' -f (Join-Path $jserverPkg 'ripple-server.jar')))) -RedirectStandardOutput $slog -RedirectStandardError ($slog + '.stderr') -PassThru
  $jProcs += $p
  "java-server$i pid=$($p.Id) api=$api ui=$ui" | Tee-Object (Join-Path $results 'pids.txt') -Append | Write-Host
}
function Wait-Port($port, $timeoutSec = 60) {
  $end = (Get-Date).AddSeconds($timeoutSec)
  while ((Get-Date) -lt $end) {
    if (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue) { return $true }
    Start-Sleep -Milliseconds 500
  }
  return $false
}
for ($i = 1; $i -le $Servers; $i++) {
  $api = $JavaApiBase + $i - 1
  if (-not (Wait-Port $api)) { throw "java server $i api port $api never listened" }
}
Start-Sleep -Seconds 3
Note "PHASE 2 END: java cluster listening"

Phase 3 'prepare the Java driver and sample server resources'
$libs = (Get-ChildItem (Join-Path $jserverPkg 'lib\*.jar') | ForEach-Object { $_.FullName }) -join ';'
$driverSrc = Join-Path $ExperimentsDir 'load\LoadDriver.java'
$driverDir = Join-Path $run 'driver'
New-Item -ItemType Directory -Force -Path $driverDir | Out-Null
Copy-Item $driverSrc (Join-Path $driverDir 'LoadDriver.java')
& $JavacResolved -encoding UTF-8 -cp "$libs;$useBuild\ripple-client\target\classes;$useBuild\ripple-common\target\classes" -d $driverDir (Join-Path $driverDir 'LoadDriver.java')
if ($LASTEXITCODE -ne 0) { throw 'javac LoadDriver failed' }
$sampleCsv = Join-Path $results 'cpu-rss.csv'
"timestamp_ms,pid,name,cpu_s,rss_mb" | Out-File $sampleCsv -Encoding utf8
function Start-Sampling($children) {
  Start-Job -ScriptBlock {
  param($pids, $csv, $untilSec)
  $end = (Get-Date).AddSeconds($untilSec)
  while ((Get-Date) -lt $end) {
    $now = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    foreach ($id in $pids) {
      try {
        $p = Get-Process -Id $id -ErrorAction Stop
        "$now,$id,$($p.ProcessName),$($p.TotalProcessorTime.TotalSeconds),$([math]::Round($p.WorkingSet64/1MB,2))" | Out-File $csv -Append -Encoding utf8
      } catch { "$now,$id,gone,0,0" | Out-File $csv -Append -Encoding utf8 }
    }
    Start-Sleep -Milliseconds 250
  }
} -ArgumentList @((@($children | ForEach-Object { $_.Id }) + @()), $sampleCsv, 600)
}
$sampler = Start-Sampling $jProcs

Note 'Java measured phase'
$javaOut = Join-Path $results 'java-summary.json'
$dbDir = Join-Path $run 'jdriverdb'
$cpRun = "$driverDir;$libs;$useBuild\ripple-client\target\classes;$useBuild\ripple-common\target\classes"
$javaArgs = @($nodesCsv, $App, $Key, $dbDir, $javaOut, "$Subscribers", "$Warmup", "$Ops", "$GapMs", "$DeadlineMs", "$ValueBytes", "$t0")
"java LoadDriver args: $($javaArgs -join ' ')" | Tee-Object (Join-Path $results 'invocation.txt') -Append | Write-Host
$driverArgs = $jvmOpts + @('-cp', $cpRun, '-Dorg.slf4j.simpleLogger.defaultLogLevel=WARN', 'LoadDriver') + $javaArgs
$quoted = $driverArgs | ForEach-Object { '"{0}"' -f $_ }
$javaDriver = Start-Process $JavaResolved -WindowStyle Hidden -ArgumentList $quoted -PassThru -RedirectStandardOutput (Join-Path $results 'java-driver.log') -RedirectStandardError (Join-Path $results 'java-driver.stderr.log')
if ($javaDriver.WaitForExit($JavaDriverTimeoutSeconds * 1000)) {
  $javaExit = $javaDriver.ExitCode
} else {
  Note "Java driver exceeded its test timeout ($JavaDriverTimeoutSeconds seconds)"
  Stop-Children @($javaDriver)
  $javaExit = 124
}
Note "PHASE 3 END: java driver exit=$javaExit"
if ($sampler) {
  Stop-Job $sampler -ErrorAction SilentlyContinue
  Remove-Job $sampler -ErrorAction SilentlyContinue
  $sampler = $null
}
Stop-Children $jProcs
$jProcs = @()

Phase 4 'C++ servers (measured phase B setup; sequential, no overlap with Java measurement)'
$cppPorts = @()
for ($i = 1; $i -le $Servers; $i++) {
  $port = $CppBase + $i - 1
  $cppPorts += $port
  $db = Join-Path $run "cpp-node$i.db"
  $slog = Join-Path $results "cpp-server$i.log"
  if ($Servers -eq 1) {
    $p = Start-Process $CippleServerExe -WindowStyle Hidden -ArgumentList @("$port", ('"{0}"' -f $db)) -RedirectStandardOutput $slog -RedirectStandardError ($slog + '.stderr') -PassThru
  } else {
    $peers = @()
    for ($j = 1; $j -le $Servers; $j++) {
      if ($j -ne $i) { $peers += "${j}@127.0.0.1:$($CppBase + $j - 1)" }
    }
    $peerStr = $peers -join ','
    $p = Start-Process $CippleServerExe -WindowStyle Hidden -ArgumentList @("$port", ('"{0}"' -f $db), '--id', "$i", '--peers', "$peerStr", '--overlay', "$CppOverlay") -RedirectStandardOutput $slog -RedirectStandardError ($slog + '.stderr') -PassThru
  }
  $cppProcs += $p
  "cpp-server$i pid=$($p.Id) port=$port" | Tee-Object (Join-Path $results 'pids.txt') -Append | Write-Host
}
Start-Sleep -Seconds 2
$cppOk = $true
for ($i = 1; $i -le $Servers; $i++) {
  $port = $CppBase + $i - 1
  if (-not (Wait-Port $port 20)) {
    $cppOk = $false
    Note "cpp server $i port $port never listened (need candidate CLI for multi-node; see cpp-server$i.log)"
  }
}
Note "PHASE 4 END: cppOk=$cppOk"

$sampler = Start-Sampling $cppProcs


Phase 5 'C++ wire live (phase B, Java has exited)'
$cppOut = Join-Path $results 'cpp-summary.json'
$cppServers = ($cppPorts | ForEach-Object { "127.0.0.1:$_" }) -join ','
"python cpp_wire_driver.py --servers $cppServers --subs $Subscribers --home roundrobin (live, index-mod)" | Tee-Object (Join-Path $results 'invocation.txt') -Append | Write-Host
if ($cppOk) {
  $oldEAP3 = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
  & $PythonResolved (Join-Path $ExperimentsDir 'load\cpp_wire_driver.py') --servers $cppServers --app $App --key $Key --subs $Subscribers --warmup $Warmup --ops $Ops --gap-ms $GapMs --deadline-ms $DeadlineMs --value-bytes $ValueBytes --t0 $t0 --home roundrobin --out $cppOut 2>&1 | Tee-Object (Join-Path $results 'cpp-driver.log')
  $cppExit = $LASTEXITCODE
  $ErrorActionPreference = $oldEAP3
  Note "PHASE 5 END: cpp live driver exit=$cppExit"
} else {
  "cpp skipped: servers not listening (need candidate CLI for multi-node)" | Tee-Object (Join-Path $results 'cpp-driver.log') | Write-Host
  Note 'PHASE 5 END: cpp skipped'
}

Phase 6 'optional C++ cursor and SQLite profiles'
$cppDurOut = Join-Path $results 'cpp-durable-summary.json'
$cppPersistOut = Join-Path $results 'cpp-persist-summary.json'
if ($ProbeDurable -and $cppOk -and $Servers -eq 1) {
  $oldEAP4 = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
  & $PythonResolved (Join-Path $ExperimentsDir 'load\cpp_wire_driver.py') --servers $cppServers --app $App --key "$Key-dur" --subs $Subscribers --warmup $Warmup --ops $Ops --gap-ms $GapMs --deadline-ms $DeadlineMs --value-bytes $ValueBytes --t0 $t0 --home roundrobin --resume-after 0 --out $cppDurOut 2>&1 | Tee-Object (Join-Path $results 'cpp-durable-driver.log')
  $durExit = $LASTEXITCODE
  $ErrorActionPreference = $oldEAP4
  Note "bare durable-cursor probe exit=$durExit (wire-only; NOT persistence-equivalent)"
} elseif ($ProbeDurable) {
  "probe-durable skipped (needs single-node listening server)" | Tee-Object (Join-Path $results 'cpp-durable-driver.log') | Write-Host
}
if ($MeasurePersist -and $cppOk) {
  $persistDir = Join-Path $run 'cpp-persist'
  New-Item -ItemType Directory -Force -Path $persistDir | Out-Null
  $oldEAP6 = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
  & $PythonResolved (Join-Path $ExperimentsDir 'load\cpp_wire_driver.py') --servers $cppServers --app $App --key "$Key-persist" --subs $Subscribers --warmup $Warmup --ops $Ops --gap-ms $GapMs --deadline-ms $DeadlineMs --value-bytes $ValueBytes --t0 $t0 --home roundrobin --resume-after 0 --persist-sqlite $persistDir --out $cppPersistOut 2>&1 | Tee-Object (Join-Path $results 'cpp-persist-driver.log')
  $persistExit = $LASTEXITCODE
  $ErrorActionPreference = $oldEAP6
  Note "persist-sqlite probe exit=$persistExit (durable cursor + per-sub SQLite commit before ACK; comparable post-persistence)"
}
Note 'PHASE 6 END'

Phase 7 'summarize (utf-8-sig tolerant; keeps live vs persist distinct)'
$compOut = Join-Path $results 'comparison.json'
$metaObj = [ordered]@{
  stamp = $stamp; ripple_pinned = $RipplePinnedSha; ripple_head = $rippleHead; cipple_ref = $cippleRef; cipple_sha256 = $cippleHash
  cipple_mtime = $cippleMtime; cipple_exe = $CippleServerExe
  servers = $Servers; subs = $Subscribers; java_proto = $JavaProtocol; cpp_overlay = $CppOverlay
  java_event_loop_threads = $JavaEventLoopThreads; java_log_level = $JavaLogLevel
  java_driver_timeout_seconds = $JavaDriverTimeoutSeconds
  app = $App; key = $Key; warmup = $Warmup; ops = $Ops; gap_ms = $GapMs
  deadline_ms = $DeadlineMs; value_bytes = $ValueBytes; t0_ms = $t0
  placement = 'index-mod: sub i -> nodes[i % N], pub -> nodes[0] (node 1)'
  java_build = $useBuild; probe_durable = [bool]$ProbeDurable; measure_persist = [bool]$MeasurePersist
  java_exit = $javaExit; cpp_exit = $cppExit; durable_exit = $durExit; persist_exit = $persistExit
}
$metaFile = Join-Path $results 'meta.json'
($metaObj | ConvertTo-Json) | Out-File $metaFile -Encoding utf8
$oldEAP5 = $ErrorActionPreference; $ErrorActionPreference = 'Continue'
$sumArgs = @((Join-Path $ExperimentsDir 'load\summarize.py'), '--java', $javaOut, '--cpp', $cppOut, '--cpu', $sampleCsv, '--out', $compOut, '--meta-file', $metaFile)
if ((Test-Path $cppDurOut)) { $sumArgs += @('--cpp-durable', $cppDurOut) }
if ((Test-Path $cppPersistOut)) { $sumArgs += @('--cpp-persist', $cppPersistOut) }
& $PythonResolved @sumArgs 2>&1 | Tee-Object (Join-Path $results 'summarize.log')
$ErrorActionPreference = $oldEAP5
Note 'PHASE 7 END'

} finally {
  Note 'cleanup: stopping only processes started by this script'
  try { Stop-Job $sampler -ErrorAction SilentlyContinue } catch {}
  try { Remove-Job $sampler -ErrorAction SilentlyContinue } catch {}
  Stop-Children $jProcs
  Stop-Children $cppProcs
  if ($javaDriver) { Stop-Children @($javaDriver) }
}

"java_exit=$javaExit cpp_exit=$cppExit durable_exit=$durExit persist_exit=$persistExit" | Tee-Object (Join-Path $results 'summary.txt') -Append | Write-Host
if (Test-Path $compOut) { Get-Content $compOut | Out-File (Join-Path $results 'summary.txt') -Append }
Write-Host "DONE -> $results"

if ($javaExit -ne 0 -or $cppExit -ne 0 -or ($MeasurePersist -and $persistExit -ne 0) -or -not (Test-Path $compOut)) {
  exit 1
}
