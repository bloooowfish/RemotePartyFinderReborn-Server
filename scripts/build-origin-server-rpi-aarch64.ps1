param()

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$TargetTriple = "aarch64-unknown-linux-gnu"
$StageDir = Join-Path $RepoRoot "output\origin-server-rpi-aarch64"
$ReleaseDir = Join-Path $RepoRoot "target\$TargetTriple\release"
$BinaryName = "RemotePartyFinder-Reborn"

function Require-Command {
  param([string]$Name)
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Required command not found: $Name"
  }
}

function Invoke-NativeQuiet {
  param(
    [string]$Command,
    [string[]]$Arguments = @()
  )

  $stdoutPath = Join-Path $env:TEMP ("origin-server-native-" + [guid]::NewGuid().ToString() + ".stdout.log")
  $stderrPath = Join-Path $env:TEMP ("origin-server-native-" + [guid]::NewGuid().ToString() + ".stderr.log")
  try {
    $process = Start-Process -FilePath $Command `
      -ArgumentList $Arguments `
      -NoNewWindow `
      -Wait `
      -PassThru `
      -RedirectStandardOutput $stdoutPath `
      -RedirectStandardError $stderrPath
    return $process.ExitCode
  }
  finally {
    if (Test-Path $stdoutPath) {
      Remove-Item $stdoutPath -Force -ErrorAction SilentlyContinue
    }
    if (Test-Path $stderrPath) {
      Remove-Item $stderrPath -Force -ErrorAction SilentlyContinue
    }
  }
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

Require-Command rustup
Require-Command cargo
Require-Command zig
Require-Command cargo-zigbuild

if ((Invoke-NativeQuiet "cargo-zigbuild" @("--version")) -ne 0) {
  throw "cargo-zigbuild is not installed. Run `cargo install cargo-zigbuild` first."
}

if ((Invoke-NativeQuiet "rustup" @("target", "add", $TargetTriple)) -ne 0) {
  throw "Failed to install Rust target $TargetTriple."
}

Push-Location $RepoRoot
try {
  & cargo zigbuild --release --target $TargetTriple --bin $BinaryName
  if ($LASTEXITCODE -ne 0) {
    throw "cargo zigbuild failed."
  }
}
finally {
  Pop-Location
}

if (Test-Path $StageDir) {
  Remove-Item $StageDir -Recurse -Force
}

New-Item -ItemType Directory -Force -Path $StageDir | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $StageDir "scripts") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $StageDir "logs") | Out-Null

Copy-Item (Join-Path $ReleaseDir $BinaryName) (Join-Path $StageDir "server") -Force
Copy-Item (Join-Path $RepoRoot "ecosystem.origin-server.config.js") (Join-Path $StageDir "ecosystem.origin-server.config.js") -Force
Copy-Item (Join-Path $RepoRoot "config.example.toml") (Join-Path $StageDir "config.example.toml") -Force
Copy-TextFileWithLf (Join-Path $RepoRoot "scripts\remote-rpi-origin-server-postdeploy.sh") (Join-Path $StageDir "scripts\remote-rpi-origin-server-postdeploy.sh")

Write-Host "Prepared Raspberry Pi origin server deployment bundle: $StageDir"
