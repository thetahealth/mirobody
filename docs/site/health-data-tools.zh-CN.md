# 健康数据工具

内置 agent 和已登录的 MCP 客户端可以查询当前用户的记录。
用户身份由会话、令牌或个人链接确定，不作为模型自行填写的工具参数。

## 选择工具

| 工具 | 查询内容 |
| --- | --- |
| `query_health_indicators` | 测量指标，以及用户记录的症状和诊断。 |
| `query_medications` | 用药计划、已记录的服药情况与疗程历史。 |
| `query_genetic_data` | 按位点、基因或区间查询基因型。 |
| `query_pharmacogenomics` | CPIC 药物与基因的数据覆盖情况。 |
| `resolve_indicator` | 离线解析指标名称。 |
| `normalize_unit`、`convert_unit` | 离线规范单位及换算数值。 |

账号没有相应数据时，MCP 会隐藏对应查询工具。
医学资料检索属于可选能力，查询公共资料，不读取个人记录。
连接方法见 [MCP 接入](https://docs.mirobody.ai/zh/tools/mcp-integration)。

## 查询指标

先找到指标名称，再选择所需视图：

```python
query_health_indicators(keywords=["cholesterol"])
query_health_indicators(indicators=["<从结果中复制的名称>"], view="latest")
```

| 视图 | 返回内容 |
| --- | --- |
| `raw` | 原始读数。 |
| `minute`、`hour`、`day`、`week`、`month` | 对应时间粒度的汇总。 |
| `stats` | 数量、最小值、最大值、均值、总和、首尾值和变化。 |
| `latest` | 每个指标的最近一次结果。 |

未提供 `keywords` 或 `indicators` 时，返回指标目录，指定的视图不会直接应用。
`start`、`end` 按记录所属用户的时区解释，包含起止当天；指标查询未指定日期时，范围为全部记录。

## 阅读结果

结果会说明状态（`ok`、`partial`、`error`）、时间范围、视图、来源及计算前提。
被截断的结果不代表完整历史，应缩小范围或改用合适的统计视图。
按日及更粗粒度的视图以天为单位统计，均值可能与逐条读数的统计结果不同。

没有数据不代表没有相关症状或疾病。引用或计算时请保留返回的行 ID；
额外计算可使用[计算工具](../python-analysis.zh-CN.md)。
完整参数与返回约定见[工具参考](../answers.md)。
