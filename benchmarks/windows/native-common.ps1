# Keep the native runners usable from a checkout, including PowerShell 5.1.
$ErrorActionPreference = "Stop"

function Find-Uv {
    $cmd = Get-Command uv.exe -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    foreach ($path in @("$env:LOCALAPPDATA\Microsoft\WinGet\Links\uv.exe",
                        "$env:USERPROFILE\.local\bin\uv.exe")) {
        if (Test-Path -LiteralPath $path -PathType Leaf) { return $path }
    }
    $found = Get-ChildItem "$env:LOCALAPPDATA\Microsoft\WinGet\Packages" -Recurse -Filter uv.exe `
        -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($found) { return $found.FullName }
    throw "uv.exe is missing; install uv or start a shell with uv on PATH."
}

function Find-Server([string]$Dir) {
    $exe = Get-ChildItem -LiteralPath $Dir -Recurse -Filter llama-server.exe `
        -ErrorAction SilentlyContinue | Sort-Object FullName | Select-Object -First 1
    if ($exe) { return $exe.FullName }
    return $null
}

function Assert-FreePort([int]$Port) {
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $pending = $client.BeginConnect("127.0.0.1", $Port, $null, $null)
        if ($pending.AsyncWaitHandle.WaitOne(1000)) {
            try { $client.EndConnect($pending) } catch { return }
            throw "Port $Port already has a listener; stop it before benchmarking."
        }
    } finally { $client.Close() }
}

function Get-LocalHealth([int]$Port) {
    # PowerShell 5.1 has no Invoke-WebRequest -NoProxy; the long-running local
    # model must not be routed through the machine's configured HTTP proxy.
    $request = [Net.HttpWebRequest]::Create("http://127.0.0.1:$Port/health")
    $request.Proxy = $null
    $request.Timeout = 1000
    $request.ReadWriteTimeout = 1000
    $response = $null
    $reader = $null
    try {
        $response = $request.GetResponse()
        $reader = New-Object IO.StreamReader($response.GetResponseStream())
        return ($reader.ReadToEnd() | ConvertFrom-Json)
    } finally {
        if ($reader) { $reader.Dispose() }
        if ($response) { $response.Dispose() }
    }
}

function Wait-Server([Diagnostics.Process]$Process, [int]$Port, [int]$Seconds = 300) {
    $timer = [Diagnostics.Stopwatch]::StartNew()
    while ($timer.Elapsed.TotalSeconds -lt $Seconds) {
        $Process.Refresh()
        if ($Process.HasExited) { throw "llama-server exited with code $($Process.ExitCode)." }
        try {
            if ((Get-LocalHealth $Port).status -eq "ok") {
                $Process.Refresh()
                if (-not $Process.HasExited) { return }
            }
        } catch { }
        Start-Sleep -Seconds 1
    }
    throw "llama-server did not become healthy within $Seconds seconds."
}

function Stop-Server([Diagnostics.Process]$Process) {
    if ($Process) {
        # Kill through the object Start-Process returned, not by id. A
        # HasExited check followed by Stop-Process -Id threw "Cannot find a
        # process" when the process exited in between (CI, PowerShell 7, the
        # health fixture exiting on its own); in Run-One's finally that error
        # replaced the one that mattered. Kill on an exited process returns on
        # .NET and throws InvalidOperationException on .NET Framework, hence
        # the catch, which rethrows anything that leaves the process running.
        try { $Process.Kill() } catch { if (-not $Process.HasExited) { throw } }
        $Process.WaitForExit()
        $Process.Dispose()
    }
}

function Require-File([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { throw "Missing file: $Path" }
}

function Invoke-Probe([string]$Uv, [string[]]$Arguments) {
    # PS 5.1 treats redirected native stderr as ErrorRecords; retain diagnostics
    # and use the process exit code rather than PowerShell's pipeline status.
    $oldPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        # A failed native launch leaves this automatic variable unchanged.
        # Its global slot avoids shadowing the value the process writes.
        $global:LASTEXITCODE = $null
        & $Uv @Arguments 2>&1 | ForEach-Object { Log "$_" }
        $code = $global:LASTEXITCODE
    } finally { $ErrorActionPreference = $oldPreference }
    if ($null -eq $code) { throw "speed_probe.py did not start; native exit code unavailable." }
    if ($code -ne 0) { throw "speed_probe.py exited with code $code." }
}
