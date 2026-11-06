# Apple Health

可以导入 iOS 健康 App 的导出包，也可以由已获用户授权的客户端推送 HealthKit 数据。
Apple Health 不提供类似设备云服务的服务端 OAuth 连接。

## 导入文件

在健康 App 中打开个人资料，选择**导出所有健康数据**。
基础 Python 包即可读取导出包，无需模型或数据库：

```bash
mirobody import apple export.zip
```

需要写入某位用户的记录时，使用已安装应用依赖、并配置好数据库的环境：

```bash
mirobody import apple export.zip --into <user-id>
```

重复导入同一文件不会新增一份副本。文件导入与手机推送有重叠时，
每日汇总会选择一个来源，不把两份数据相加。

每条读数都计入手机当时显示的那一天：Apple 给每个时间都写了时区偏移，
导入按这个偏移划分日期。用旧版本导入过的文件（未指定 `--tz` 时按 UTC 划分日期），
重新导入一次即可更正。

## 客户端推送

已登录客户端通过 `POST /apple/health` 发送 HealthKit JSON。
使用 Apple 的原始标识符，例如 `HKQuantityTypeIdentifierHeartRate`，
并逐条发送原始单位、`startDate` 和 `endDate`。

- 日期可使用毫秒时间戳，或带时区偏移的 Apple 日期字符串。
- 百分比类型使用小数，例如 98% 血氧饱和度发送为 `0.98`。
- 分类值使用对应的 `HKCategoryValue*` 字符串。
- 实时推送前由客户端去重。

CDA 文档使用应用的 CDA 导入流程。
支持类型、请求示例和校验规则见 [Apple Health 技术参考](../apple-health.md)。
