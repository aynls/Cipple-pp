param(
  [string]$BuildDirectory = "build/windows-release",
  [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$build = [IO.Path]::GetFullPath($BuildDirectory, $repo)
if (-not $SkipBuild) {
  cmake --preset windows-release -S $repo -B $build
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
  cmake --build $build --target cipple_server cipple_client --parallel 6
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

py -3 (Join-Path $PSScriptRoot 'demo.py') --server (Join-Path $build 'server/cipple_server.exe') --client (Join-Path $build 'client/cipple_client.exe')
exit $LASTEXITCODE
