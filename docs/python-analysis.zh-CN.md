# 聊天中的计算

**[English](python-analysis.md)** · **中文** · **[日本語](python-analysis.ja.md)**

`code_exec` 用于对已查询的健康记录进行计算，也可比较多次查询得到的健康指标、用药、基因型和药物基因组数据。工具会自动传入原始数据，模型无需从展示表格中复制数值。模型写哪种语言取决于模型在哪里运行，工具名称不变。

| 聊天模型运行在 | 代码的执行方式 | 需要的配置 |
|---|---|---|
| 云端服务 | JavaScript，在应用内的引擎中执行 | 无 |
| 本机或你自己的网络 | Python，在本地 Docker 容器中执行 | 应用能连接的 Docker 服务；连接不上时改用 JavaScript |

[计算基准测试](../benchmarks/local_agent/eval_language_results.md)中，云端模型用两种语言的正确率相同。JavaScript 不需要容器，所以无论怎样部署（包括只用主 Compose 配置），云端模型都有计算工具可用。

## 输入与输出

代码可以使用：

- `data`：最近一次成功且非空的记录查询返回的原始数据行。
- `results`：当前轮次内的成功查询结果，包含元数据、假设、来源和工具调用 ID。
- 仅 Python：`/workspace/data.json`，计算容器中的同一份 `data` 和 `results`，不可作为文档上传或读取路径。

当前轮次从用户最近一次提问开始，自动修复回答的指令仍属于这一轮。最近一次查询失败或为空时，工具会拒绝计算，避免沿用旧数据或将缺失值当作零。查询工具的 `stats` 和时间分桶视图已能提供的统计，直接使用对应视图即可。

代码应输出计算结果及来源 `rid`：Python 用 `print(...)`，JavaScript 用 `console.log(...)`，JavaScript 最后一个表达式的值也会一并返回。工具会报告退出状态、源查询 ID，以及输入或输出是否完整。输出上限为 40,000 个字符，超过部分会明确标记为不完整。

## JavaScript

执行引擎是编译为 WebAssembly 的 QuickJS，在应用进程内由 wasmtime 运行，也就是 Deep Agents [解释器](https://docs.langchain.com/oss/python/deepagents/interpreters)底层的引擎，但不带它的工具桥接，代码只能读到注入的数据行。每次调用都使用全新的引擎，内存上限 128 MB，执行期限 1 至 30 秒，主机最多额外等待 10 秒。可以使用标准内置对象（`Math`、`JSON`、`Date`），不能访问网络、文件、进程，也没有 `require` 和 `import`。无需任何配置。

## Python

### 本地配置

源码安装时，先安装应用依赖并下载计算镜像：

```bash
pip install -e '.[app,test]'
docker pull python:3.12-slim
```

支持 macOS 的 Docker Desktop、Colima，以及 Linux 的 Docker Engine。执行器依次检查 `PYTHON_SANDBOX_DOCKER_HOST`、`DOCKER_HOST`、`/var/run/docker.sock` 和当前 Docker CLI context，只接受本地 Unix socket，拒绝 TCP 和 SSH 地址。

默认镜像仅提供 Python 标准库。如需其他分析包，请预先构建本地镜像，并通过 `PYTHON_SANDBOX_IMAGE` 指定。依赖必须提前安装，因为计算容器不能访问网络。

### 应用本身也在 Docker 中运行时

应用需要连接主机的 Docker 服务，才能创建计算容器。可选配置 [compose-python.yaml](../compose-python.yaml) 会将管理 socket 挂载给应用，授予其管理容器的权限，因此需要主动启用；主 Compose 配置不会默认挂载。计算容器不会获得该 socket。

将 `PYTHON_DOCKER_SOCKET` 设为 Docker 主机上的 socket 路径，将 `PYTHON_DOCKER_GID` 设为 Docker 服务看到的所属组 ID（Colima 为 991），可用以下命令查看：

```bash
docker run --rm -v /var/run/docker.sock:/s python:3.12-slim stat -c %g /s
```

然后执行：

```bash
docker compose -f compose.yaml -f compose-python.yaml up -d
```

### 无法连接 Docker 服务时

应用只在启动时检查一次 Docker 服务，而且只在有聊天模型运行在本机或你自己的网络时才检查。检查失败时（只用主 Compose 配置、应用不在 socket 所属组、Docker 未运行），本地模型也改用 JavaScript 计算，日志只记录一次原因：

```text
code_exec runs JavaScript for local models too: no local Docker daemon answered: error_type=FileNotFoundError (docs/python-analysis.md)
```

修正配置后需重启应用。如果应用运行期间 Docker 服务停止，工具会提示配置问题，已有查询结果仍然有效。

## 关闭计算工具

设置 `DISALLOWED_TOOLS: [code_exec]`。工具以前的名称 `python_exec` 和 `eval` 会按 `code_exec` 读取，并在日志中给出提示。

## Python 容器的运行约束

计算容器不挂载应用文件，不接收数据库凭据，也不能访问网络。容器使用 UID/GID 65534、只读镜像和临时可写存储，限制为一个 CPU、512 MB 内存、最多 64 个进程。命令执行期限为 1 至 30 秒，主机最多额外等待 10 秒。超时或取消后会关闭输出流并移除容器；有限租期和自动删除机制也会清理遗留任务。模型只能调用计算工具，不能执行通用 shell 命令。

实现与验证细节见[计算基准测试](../benchmarks/local_agent/README.md)和 [Deep Agents 沙箱接口](https://docs.langchain.com/oss/python/deepagents/sandboxes#the-execute-method)。
