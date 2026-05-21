param(
    [Parameter(Mandatory = $true)]
    [string]$Config
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir

Push-Location $projectRoot
try {
    go run .\cmd\kafka-json-loader -config $Config
}
finally {
    Pop-Location
}
