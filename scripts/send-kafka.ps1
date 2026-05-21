param(
    [Parameter(Mandatory = $true)]
    [string]$Brokers,

    [Parameter(Mandatory = $true)]
    [string]$Topic,

    [int]$Count = 1,
    [string]$KeyPrefix = "msg",
    [string]$EventType = "generated",
    [string]$Source = "kafka-json-loader",
    [switch]$Ssl,
    [string]$SslServerName,
    [string]$SslCaFile,
    [string]$SslCertFile,
    [string]$SslKeyFile,
    [switch]$SslInsecureSkipVerify
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir

Push-Location $projectRoot
try {
    $args = @(
        "run", ".\cmd\kafka-json-loader",
        "-brokers", $Brokers,
        "-topic", $Topic,
        "-count", $Count,
        "-key-prefix", $KeyPrefix,
        "-event-type", $EventType,
        "-source", $Source
    )

    if ($Ssl) {
        $args += @("-ssl")
    }
    if ($SslServerName) {
        $args += @("-ssl-server-name", $SslServerName)
    }
    if ($SslCaFile) {
        $args += @("-ssl-ca-file", $SslCaFile)
    }
    if ($SslCertFile) {
        $args += @("-ssl-cert-file", $SslCertFile)
    }
    if ($SslKeyFile) {
        $args += @("-ssl-key-file", $SslKeyFile)
    }
    if ($SslInsecureSkipVerify) {
        $args += @("-ssl-insecure-skip-verify")
    }

    go @args
}
finally {
    Pop-Location
}
