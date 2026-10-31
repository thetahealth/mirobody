# Calculation in chat

[中文](python-analysis.zh-CN.md)

`code_exec` lets chat models calculate over queried health records, including
comparisons across indicators, medications, genetics and pharmacogenomics.
It receives the original data automatically, so the model does not need to
copy values from a displayed table. Which language the model writes depends
on where the model runs; the tool's name does not change.

| The chat model runs | The code runs as | Setup |
|---|---|---|
| at a cloud service | JavaScript, in an engine inside the app | none |
| on this machine or your own network | Python, in a local Docker container | a Docker daemon the app can reach; without one, JavaScript |

The comparison in the [calculation benchmarks](../benchmarks/local_agent/eval_language_results.md)
found the cloud models equally correct in both languages. JavaScript needs no
container, so a cloud model has a calculator in every deployment, including
the main Compose file alone.

## Inputs and outputs

The code can use:

- `data`: original rows from the latest successful, nonempty record query.
- `results`: successful query results from the current turn, including
  metadata, assumptions, source information and tool-call IDs.
- In Python only, `/workspace/data.json`: the same `data` and `results`
  inside the calculation container. This is not a path for uploading or
  reading documents.

The current turn starts with the latest user question; automatic requests to
repair an answer remain part of that turn. If the latest query fails or
returns no rows, calculation is refused rather than using older data or
substituting zero. Use query `stats` and time-bucket views for the aggregates
they already provide.

Code should print its result and source `rid`s: `print(...)` in Python,
`console.log(...)` in JavaScript, where the value of a final expression is
returned as well. The tool reports exit status, source-call IDs and incomplete
inputs or output. Output is limited to 40,000 characters; longer output is
explicitly marked incomplete.

## JavaScript

The engine is QuickJS compiled to WebAssembly and run on wasmtime inside the
app process: the one under the Deep Agents
[interpreter](https://docs.langchain.com/oss/python/deepagents/interpreters),
without its tool bridge, so code reads only the injected rows. Each call gets
a fresh engine with 128 MB of memory and a 1–30 second deadline, with up to
ten additional seconds for the host to wait. The standard built-ins (`Math`,
`JSON`, `Date`) are there; the network, files, a process, `require` and
`import` are not. There is nothing to set up.

## Python

### Local setup

For a source installation, install the application dependencies and download
the calculation image:

```bash
pip install -e '.[app,test]'
docker pull python:3.12-slim
```

Supported runtimes are Docker Desktop or Colima on macOS, and Docker Engine
on Linux. The executor checks `PYTHON_SANDBOX_DOCKER_HOST`, `DOCKER_HOST`,
`/var/run/docker.sock`, then the active Docker CLI context. Only local Unix
sockets are accepted; TCP and SSH endpoints are rejected.

The default image provides Python's standard library. To add analysis
packages, prepare a local image and set `PYTHON_SANDBOX_IMAGE`. Install its
dependencies in advance: calculation containers have no network access.

### When the app also runs in Docker

The app needs access to the host's Docker daemon to start calculation
containers. The optional [compose-python.yaml](../compose-python.yaml) mounts
the management socket into the app, granting it permission to manage
containers. This is opt-in; the main Compose file does not mount the socket.
Calculation containers never receive it.

Set `PYTHON_DOCKER_SOCKET` to the socket path on the Docker host and
`PYTHON_DOCKER_GID` to its owning group ID as the daemon sees it (Colima
reports 991), which this prints:

```bash
docker run --rm -v /var/run/docker.sock:/s python:3.12-slim stat -c %g /s
```

Then run:

```bash
docker compose -f compose.yaml -f compose-python.yaml up -d
```

### When no daemon answers

The app asks the daemon once, at startup, and only when a chat model runs on
this machine or your own network. When nothing answers (the main Compose file
alone, a socket whose group the app is not in, Docker not running), local
models compute in JavaScript too, and the log says so once:

```text
code_exec runs JavaScript for local models too: no local Docker daemon answered: error_type=FileNotFoundError (docs/python-analysis.md)
```

Restart the app after fixing the setup. If the daemon stops while the app
runs, the tool reports a setup problem and the queried readings remain valid.

## Turning it off

Set `DISALLOWED_TOOLS: [code_exec]`. The tool's earlier names, `python_exec`
and `eval`, are read as `code_exec`, with a warning in the log.

<a id="runtime-and-checks"></a>

## Python container limits

Calculation containers have no application mounts, database credentials or
network. They run as UID/GID 65534 with a read-only image and temporary
writable storage, limited to one CPU, 512 MB of memory and 64 processes.
Commands have a 1–30 second deadline, with up to ten additional seconds for
the host to wait. Timeout or cancellation closes the stream and removes the
container; a finite lease and automatic removal also clean up abandoned runs.
Only code execution is exposed to the model, not a general shell tool.

For implementation and validation details, see the
[calculation benchmarks](../benchmarks/local_agent/README.md) and
[Deep Agents sandbox interface](https://docs.langchain.com/oss/python/deepagents/sandboxes#the-execute-method).
