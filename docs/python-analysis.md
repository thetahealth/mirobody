# Local Python analysis

[中文](python-analysis.zh-CN.md)

`python_exec` lets chat models calculate over queried health records, including
comparisons across indicators, medications, genetics and pharmacogenomics.
It receives the original data automatically, so the model does not need to
copy values from a displayed table. A cloud model can write the code, but the
code runs in a local Docker container.

## Inputs and outputs

The code can use:

- `data`: original rows from the latest successful, nonempty record query.
- `results`: successful query results from the current turn, including
  metadata, assumptions, source information and tool-call IDs.
- `/workspace/data.json`: the same `data` and `results` inside the calculation
  container. This is not a path for uploading or reading documents.

The current turn starts with the latest user question; automatic requests to
repair an answer remain part of that turn. If the latest query fails or
returns no rows, calculation is refused rather than using older data or
substituting zero. Use query `stats` and time-bucket views for the aggregates
they already provide.

Code should print its result and source `rid`s. The tool reports exit status,
source-call IDs and incomplete inputs or output. Output is limited to
40,000 bytes; longer output is explicitly marked incomplete.

## Local setup

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

The app asks the daemon once, at startup. When nothing answers (the main
Compose file alone, a socket whose group the app is not in, Docker not
running), chat models are not offered `python_exec` and the system prompt does
not mention it; the query views' own statistics still answer. The log says so
once:

```text
python_exec left out: no local Docker daemon answered: error_type=FileNotFoundError (docs/python-analysis.md)
```

Restart the app after fixing the setup. If the daemon stops while the app
runs, the tool reports a setup problem and the queried readings remain valid.
To hide the calculator even when a daemon answers, set
`DISALLOWED_TOOLS: [python_exec]`.

<a id="runtime-and-checks"></a>

## Execution limits

Calculation containers have no application mounts, database credentials or
network. They run as UID/GID 65534 with a read-only image and temporary
writable storage, limited to one CPU, 512 MB of memory and 64 processes.
Commands have a 1–30 second deadline, with up to ten additional seconds for
the host to wait. Timeout or cancellation closes the stream and removes the
container; a finite lease and automatic removal also clean up abandoned runs.
Only Python execution is exposed to the model, not a general shell tool.

For implementation and validation details, see the
[calculation benchmarks](../benchmarks/local_agent/README.md) and
[Deep Agents sandbox interface](https://docs.langchain.com/oss/python/deepagents/sandboxes#the-execute-method).
