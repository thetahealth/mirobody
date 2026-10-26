# Agent 技能

为编码助手安装技能后，可以直接使用 Mirobody 的操作流程与命令。
这些技能运行在外部助手中，与应用自带的聊天 agent 分开。

| 技能 | 用途 |
| --- | --- |
| `standardize-health-data` | 使用 Python 库规范读数、读取健康文档。 |
| `mirobody` | 启动应用，并让助手通过 MCP 连接。 |

## 安装

```bash
npx skills add thetahealth/mirobody --skill standardize-health-data
npx skills add thetahealth/mirobody --skill mirobody
```

库调用需要 Python 3.12+ 和 `mirobody`；读取文档还需解析依赖与模型密钥。
应用操作需要 Docker，以及已配置的云端或本地模型。

已有部署时，也可以直接按 [MCP 接入](https://docs.mirobody.ai/zh/tools/mcp-integration)连接。
其他安装方式见[技能参考](../../skills/README.md)。
