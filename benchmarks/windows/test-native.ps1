# No Pester dependency: also executed by Windows PowerShell 5.1 in CI.
$ErrorActionPreference = "Stop"
. "$PSScriptRoot\native-common.ps1"
$script:Passed = 0
function Log([string]$Line) { Write-Host $Line }
function Assert([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw $Message }
    $script:Passed++
}
function Assert-Throws([scriptblock]$Action, [string]$Expected) {
    $message = $null
    $stack = "no exception"
    try { & $Action } catch { $message = $_.Exception.Message; $stack = $_.ScriptStackTrace }
    Assert ($message -and $message -match $Expected) "Expected failure matching $Expected; got $message; $stack"
}
# A second handle to a child, so a check after Stop-Server (which disposes the
# caller's object) asks that exact process whether it exited. Get-Process -Id
# cannot answer that on Windows PowerShell 5.1: .NET Framework looks the id up
# in the system process list, and on windows-latest that list still held the
# child in 9 of 200 tries after WaitForExit had returned, for up to 82 ms.
# PowerShell 7 asks the process itself and never returned it.
function Watch-Process([Diagnostics.Process]$Process) {
    $watch = [Diagnostics.Process]::GetProcessById($Process.Id)
    $null = $watch.Handle  # opened now and kept, so a reused id cannot answer
    return $watch
}

# Parser errors are platform independent, even though CIM/process flags are Windows-specific.
foreach ($file in @("native-common.ps1", "speed_windows.ps1", "cpu-thread-sweep.ps1")) {
    $tokens = $null
    $errors = $null
    $null = [Management.Automation.Language.Parser]::ParseFile((Join-Path $PSScriptRoot $file), [ref]$tokens, [ref]$errors)
    Assert ($errors.Count -eq 0) "Parser errors in ${file}: $errors"
}
$missing = Join-Path ([IO.Path]::GetTempPath()) ([guid]::NewGuid().ToString())
Assert-Throws { Require-File $missing } "Missing file"
Assert ($null -eq (Find-Server $missing)) "Missing build should resolve to null."
& {
    function Get-Command { param($Name, $ErrorAction); return $null }
    function Test-Path { param($LiteralPath, $PathType); return $false }
    function Get-ChildItem { param($Path, [switch]$Recurse, $Filter, $ErrorAction); return $null }
    Assert-Throws { Find-Uv } "uv.exe is missing"
}
$listener = New-Object System.Net.Sockets.TcpListener([Net.IPAddress]::Loopback, 0)
try {
    $listener.Start()
    $port = $listener.LocalEndpoint.Port
    Assert-Throws { Assert-FreePort $port } "already has a listener"
} finally { $listener.Stop() }
Assert-FreePort $port
$script:Passed++

# A real child provides HasExited and cleanup behavior without llama.cpp/hardware.
$interpreter = (Get-Process -Id $PID).Path
$healthFixture = Join-Path ([IO.Path]::GetTempPath()) ([guid]::NewGuid().ToString())
$null = New-Item -ItemType Directory -Path $healthFixture
$portFile = Join-Path $healthFixture "port.txt"
$healthScript = @'
$listener = New-Object Net.Sockets.TcpListener([Net.IPAddress]::Loopback, 0)
$listener.Start()
try {
    [IO.File]::WriteAllText('PORT_FILE', [string]$listener.LocalEndpoint.Port)
    $client = $listener.AcceptTcpClient()
    try {
        $stream = $client.GetStream()
        $buffer = New-Object byte[] 4096
        $null = $stream.Read($buffer, 0, $buffer.Length)
        $body = '{"status":"ok"}'
        $reply = "HTTP/1.1 200 OK`r`nContent-Type: application/json`r`nContent-Length: $($body.Length)`r`nConnection: close`r`n`r`n$body"
        $bytes = [Text.Encoding]::ASCII.GetBytes($reply)
        $stream.Write($bytes, 0, $bytes.Length)
        $stream.Flush()
    } finally { $client.Close() }
} finally { $listener.Stop() }
'@
$healthScript = $healthScript.Replace("PORT_FILE", $portFile.Replace("'", "''"))
$encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($healthScript))
$healthProcess = Start-Process -FilePath $interpreter -ArgumentList "-NoProfile -EncodedCommand $encoded" -PassThru
$oldProxy = [Net.WebRequest]::DefaultWebProxy
try {
    $timer = [Diagnostics.Stopwatch]::StartNew()
    while (-not (Test-Path -LiteralPath $portFile) -and $timer.Elapsed.TotalSeconds -lt 15) { Start-Sleep -Milliseconds 100 }
    Assert (Test-Path -LiteralPath $portFile) "Local health fixture did not start."
    $healthPort = [int]([IO.File]::ReadAllText($portFile))
    # .NET Framework's WebProxy always exempts loopback even with local bypass
    # disabled; an explicit IWebProxy makes this fault real on PS 5.1 too.
    if (-not ("MirobodyBenchmarkBrokenProxy" -as [type])) {
        Add-Type -TypeDefinition @'
using System;
using System.Net;
public sealed class MirobodyBenchmarkBrokenProxy : IWebProxy {
    public ICredentials Credentials { get; set; }
    public Uri GetProxy(Uri destination) { return new Uri("http://127.0.0.1:1"); }
    public bool IsBypassed(Uri host) { return false; }
}
'@
    }
    $brokenProxy = New-Object MirobodyBenchmarkBrokenProxy
    [Net.WebRequest]::DefaultWebProxy = $brokenProxy
    Assert (-not $brokenProxy.IsBypassed([uri]"http://127.0.0.1:$healthPort/health")) "Broken proxy does not cover the health URL."
    Assert ((Get-LocalHealth $healthPort).status -eq "ok") "Local health was sent through the machine proxy."
} finally {
    [Net.WebRequest]::DefaultWebProxy = $oldProxy
    Stop-Server $healthProcess
    Remove-Item -LiteralPath $healthFixture -Recurse -Force
}
$process = Start-Process -FilePath $interpreter -ArgumentList '-NoProfile -Command "Start-Sleep -Seconds 60"' -PassThru
$child = Watch-Process $process
try {
    & {
        function Get-LocalHealth { param($Port); return @{ status = "ok" } }
        Wait-Server $process $port 3
        $script:Passed++
    }
    & {
        function Get-LocalHealth { param($Port); return @{ status = "not_ok"; token = "ok" } }
        Assert-Throws { Wait-Server $process $port 1 } "did not become healthy"
    }
    Assert-Throws { Wait-Server $process $port 0 } "did not become healthy"
} finally { Stop-Server $process }
Assert $child.HasExited "Child survived Stop-Server."
$child.Dispose()
$exited = Start-Process -FilePath $interpreter -ArgumentList '-NoProfile -Command "exit 7"' -PassThru
try {
    $exited.WaitForExit()
    Assert-Throws { Wait-Server $exited $port 3 } "exited with code 7"
    # Stop-Server meets an exited process the way it meets one that exits
    # while it is being stopped: Kill finds it gone. That must not throw.
} finally { Stop-Server $exited }
if ($env:OS -eq "Windows_NT") {
    Assert-Throws { Invoke-Probe $env:ComSpec @("/c", "exit", "17") } "exited with code 17"
    Invoke-Probe $env:ComSpec @("/c", "echo", "probe-success")
} else {
    Assert-Throws { Invoke-Probe "/bin/sh" @("-c", "exit 17") } "exited with code 17"
    Invoke-Probe "/bin/sh" @("-c", "echo probe-success")
}
$script:Passed++

# Execute the complete Run-One function with a real failing native probe and a
# sleeping child. This verifies the caller propagates failure and cleans up.
$fixture = Join-Path ([IO.Path]::GetTempPath()) ([guid]::NewGuid().ToString())
$null = New-Item -ItemType Directory -Path $fixture
try {
    & {
        $Results = $fixture
        # Windows rejects port zero before the deliberately failing probe runs.
        $Port = $port
        $Model = Join-Path $fixture "synthetic.gguf"
        $Template = Join-Path $fixture "synthetic.jinja"
        $Probe = Join-Path $fixture "probe.py"
        $Cases = Join-Path $fixture "cases.json"
        $null = New-Item -ItemType File -Path $Cases
        $LASTEXITCODE = 0
        Assert-Throws { Invoke-Probe (Join-Path $fixture "absent-uv.exe") @() } "."
        $badExecutable = Join-Path $fixture "invalid-uv.exe"
        if ($env:OS -eq "Windows_NT") {
            [IO.File]::WriteAllText($badExecutable, "not an executable")
        } else {
            [IO.File]::WriteAllText($badExecutable, "#!/missing-interpreter`n")
            & chmod +x $badExecutable
        }
        $LASTEXITCODE = 0
        Assert-Throws { Invoke-Probe $badExecutable @() } "."
        if ($env:OS -eq "Windows_NT") {
            $Uv = Join-Path $fixture "uv.cmd"
            Set-Content -LiteralPath $Uv -Value "@echo off`r`necho deliberate probe failure`r`nexit /b 17" -Encoding ASCII
        } else {
            $Uv = Join-Path $fixture "uv.sh"
            Set-Content -LiteralPath $Uv -Value "#!/bin/sh`necho deliberate probe failure`nexit 17" -Encoding ASCII
            & chmod +x $Uv
        }
        function Get-CimInstance { param($Class); return [pscustomobject]@{ LoadPercentage = 0 } }
        function Get-VramMb { return $null }
        function Wait-Server { param($Process, $Port); return }
        function Start-Process {
            param($FilePath, $ArgumentList, [switch]$PassThru, $WindowStyle, $RedirectStandardError, $RedirectStandardOutput)
            $started = Microsoft.PowerShell.Management\Start-Process -FilePath $interpreter `
                -ArgumentList '-NoProfile -Command "Start-Sleep -Seconds 60"' -PassThru
            $script:RunnerChild = Watch-Process $started
            return $started
        }
        $tokens = $null
        $errors = $null
        $ast = [Management.Automation.Language.Parser]::ParseFile((Join-Path $PSScriptRoot "speed_windows.ps1"), [ref]$tokens, [ref]$errors)
        $runOne = $ast.Find({ param($Node) $Node -is [Management.Automation.Language.FunctionDefinitionAst] -and $Node.Name -eq "Run-One" }, $true)
        . ([scriptblock]::Create($runOne.Extent.Text))
        Assert-Throws { Run-One "synthetic" $interpreter $Cases "" } "speed_probe.py exited with code 17"
        Assert $script:RunnerChild.HasExited "Run-One left a child after probe failure."
        $script:RunnerChild.Dispose()
        Assert-Throws { Run-One "missing" "" $Cases "" } "No llama-server.exe"
    }
} finally {
    Remove-Item -LiteralPath $fixture -Recurse -Force
}
Write-Host "$script:Passed native helper checks passed."
# CI's -Command wrapper otherwise inherits the deliberate failure's exit code.
exit 0
