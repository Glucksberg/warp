param(
    [string]$ProtocPath = "$env:APPDATA\npm\protoc.cmd",
    [switch]$SkipCargoCheck
)

$ErrorActionPreference = "Stop"

function Invoke-Step {
    param(
        [string]$Name,
        [scriptblock]$Command
    )

    Write-Host ""
    Write-Host "==> $Name"
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "$Name failed with exit code $LASTEXITCODE"
    }
}

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $repoRoot

if (-not (Test-Path $ProtocPath)) {
    throw "protoc not found at '$ProtocPath'. Pass -ProtocPath or install protoc.cmd under npm."
}

$env:PROTOC = (Resolve-Path $ProtocPath).Path

Write-Host "Warp local Pi runtime check"
Write-Host "Repo: $repoRoot"
Write-Host "PROTOC: $env:PROTOC"

Invoke-Step "cargo test -p warp pi_local --lib --features gui" {
    cargo test -p warp pi_local --lib --features gui
}

if (-not $SkipCargoCheck) {
    Invoke-Step "cargo check -p warp --bin warp-oss --features gui" {
        cargo check -p warp --bin warp-oss --features gui
    }
}

Invoke-Step "git diff --check" {
    git diff --check
}

Write-Host ""
Write-Host "Pi runtime check passed."
