# デバイスのフィールド対応表

デバイスのベンダーは、似た測定値にも異なる名称や定義を使います。Mirobody の対応表は、ベンダーのフィールドを内部の指標に対応づけ、確認済みの一致がある場合は LOINC コードにも対応づけます。

## 対応状況

| 状態 | 意味 |
| --- | --- |
| `confident` | ベンダーの定義が、確認済みの LOINC の軸と一致します。 |
| `unverified` | 一致する可能性はありますが、確認が必要です。 |
| コードなし | このフィールドに適切な対応がありません。理由は保持されます。 |

名称が一致するだけでは不十分です。例えば SDNN と RMSSD は、どちらも HRV と呼ばれていても異なる心拍変動の統計量であり、同じコードを共有してはいけません。ベンダー独自のスコアには、標準の対応先がないこともあります。

## Python での検索

この対応表は基本の Python パッケージに同梱されており、API キーは不要です。

```python
from mirobody.standardize import devices

field = devices.lookup("apple", "HKQuantityTypeIdentifier", "stepCount")
print(field.metric, field.loinc, field.confidence)
```

この対応表は対応している語彙を示すものであり、ある人のデバイスが実際にどのデータを記録しているかを示すものではありません。また、すべてのベンダーにすぐ使えるクラウド連携があることも意味しません。アプリケーションで使える連携は[デバイスとの接続](https://docs.mirobody.ai/ja/providers/using-providers)を参照してください。

完全な対応表と判断の根拠は、[対応表のデータ](../../mirobody/res/crosswalks/)と[技術リファレンス](../device-crosswalk.md)（英語）を参照してください。
