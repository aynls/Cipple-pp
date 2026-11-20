param(
  [string]$BuildDirectory = "build",
  [string]$Ucrt64 = $env:UCRT64_ROOT,
  [string]$Ninja = "ninja",
  [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$build = [IO.Path]::GetFullPath($BuildDirectory, $repo)
if (-not $Ucrt64) {
  $compiler = Get-Command g++ -ErrorAction Stop
  $Ucrt64 = Split-Path (Split-Path $compiler.Source)
}
$env:PATH = "$(Join-Path $Ucrt64 'bin');$env:PATH"

if (-not $SkipBuild) {
  cmake -S $repo -B $build -G Ninja "-DCMAKE_MAKE_PROGRAM=$Ninja" "-DCMAKE_CXX_COMPILER=$(Join-Path $Ucrt64 'bin/g++.exe')" "-DCMAKE_PREFIX_PATH=$Ucrt64" -DCMAKE_BUILD_TYPE=Release
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
  cmake --build $build --target cipple_server cipple_client --parallel 6
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

py -3 (Join-Path $PSScriptRoot 'demo.py') --server (Join-Path $build 'server/cipple_server.exe') --client (Join-Path $build 'client/cipple_client.exe')
exit $LASTEXITCODE
