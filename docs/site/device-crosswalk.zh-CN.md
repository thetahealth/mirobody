# 设备字段对照

不同设备可能用不同名称表示相似指标，也可能用同一名称表示不同的统计量。
Mirobody 的对照表将厂商字段映射到内部指标；确认含义一致后，再关联 LOINC 编码。

## 映射状态

| 状态 | 含义 |
| --- | --- |
| `confident` | 厂商定义与已核对的 LOINC 维度一致。 |
| `unverified` | 可能对应，但还需要确认。 |
| 无编码 | 没有合适的标准对应项，保留原因说明。 |

不能仅凭名称相似就合并数据。例如，SDNN 和 RMSSD 虽然都属于 HRV，却是不同统计量。
设备厂商自定义的评分，也可能没有对应的标准编码。

## 在 Python 中查询

基础 Python 包已包含对照表，无需模型密钥：

```python
from mirobody.standardize import devices

field = devices.lookup("apple", "HKQuantityTypeIdentifier", "stepCount")
print(field.metric, field.loinc, field.confidence)
```

对照表说明可识别的字段，不代表每个人的设备都记录这些数据，也不代表每个厂商都有现成的云端连接。
应用支持的连接方式见[设备接入](https://docs.mirobody.ai/zh/providers/using-providers)。

完整字段、出处和映射依据见[对照表数据](../../mirobody/res/crosswalks/)及[技术说明](../device-crosswalk.md)。
