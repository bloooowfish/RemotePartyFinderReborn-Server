param(
  [string]$RemoteHost = "",
  [string]$RemoteUser = "",
  [int]$Port = 22,
  [string]$AppDir = "",
  [string]$KeyPath = "",
  [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$StageDir = Join-Path $RepoRoot "output\origin-server-rpi-aarch64"
$ArchivePath = Join-Path $RepoRoot "output\origin-server-rpi-aarch64.tar"

if (-not $RemoteHost) { $RemoteHost = $env:RPI_HOST }
if (-not $RemoteUser) { $RemoteUser = $env:RPI_USER }
if (-not $AppDir) { $AppDir = $env:RPI_ORIGIN_SERVER_APP_DIR }
if (-not $KeyPath) { $KeyPath = $env:RPI_SSH_KEY }
if (-not $AppDir -and $RemoteUser) { $AppDir = "/home/$RemoteUser/app/rpf-origin-server" }

if (-not $RemoteHost -or -not $RemoteUser) {
  throw "RemoteHost and RemoteUser are required. Use -RemoteHost/-RemoteUser or set RPI_HOST/RPI_USER."
}

function Copy-TextFileWithLf {
  param(
    [string]$Source,
    [string]$Destination
  )

  $content = Get-Content -Raw -LiteralPath $Source
  $content = $content -replace "`r`n", "`n" -replace "`r", "`n"
  $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
  [System.IO.File]::WriteAllText($Destination, $content, $utf8NoBom)
}

if (-not $SkipBuild) {
  & (Join-Path $RepoRoot "scripts\build-origin-server-rpi-aarch64.ps1")
  if ($LASTEXITCODE -ne 0) {
    throw "Raspberry Pi origin server cross-build failed."
  }
}

if (-not (Test-Path $StageDir)) {
  throw "Missing staged bundle at: $StageDir"
}

$Target = "$RemoteUser@$RemoteHost"
$SshArgs = @("-p", "$Port")
$ScpArgs = @("-P", "$Port")
if ($KeyPath) {
  $SshArgs += @("-i", $KeyPath)
  $ScpArgs += @("-i", $KeyPath)
}

if (Test-Path $ArchivePath) {
  Remove-Item $ArchivePath -Force
}

& ssh @SshArgs $Target "mkdir -p '$AppDir'"
if ($LASTEXITCODE -ne 0) {
  throw "Failed to prepare remote app directory: $AppDir"
}

Push-Location $StageDir
try {
  & tar.exe -cf $ArchivePath .
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to create deployment archive."
  }
}
finally {
  Pop-Location
}

Push-Location $RepoRoot
try {
  & scp @ScpArgs "output/origin-server-rpi-aarch64.tar" "${Target}:${AppDir}.deploy.tar"
  if ($LASTEXITCODE -ne 0) { throw "Failed to upload deployment archive." }

  $PostDeployUpload = Join-Path ([System.IO.Path]::GetTempPath()) ("rpf-origin-server-postdeploy-" + [guid]::NewGuid().ToString() + ".sh")
  try {
    Copy-TextFileWithLf (Join-Path $RepoRoot "scripts\remote-rpi-origin-server-postdeploy.sh") $PostDeployUpload
    & scp @ScpArgs $PostDeployUpload "${Target}:${AppDir}.postdeploy.sh"
    if ($LASTEXITCODE -ne 0) { throw "Failed to upload remote postdeploy script." }
  }
  finally {
    if (Test-Path $PostDeployUpload) {
      Remove-Item $PostDeployUpload -Force -ErrorAction SilentlyContinue
    }
  }
}
finally {
  Pop-Location
}

& ssh @SshArgs $Target "chmod +x '$AppDir.postdeploy.sh' && bash '$AppDir.postdeploy.sh' '$AppDir' '${AppDir}.deploy.tar'"
if ($LASTEXITCODE -ne 0) {
  throw "Remote deploy/restart sequence failed."
}

if (Test-Path $ArchivePath) {
  Remove-Item $ArchivePath -Force
}

Write-Host "Origin server deployment completed: ${Target}:$AppDir"
