"""Regenerate the localized hero diagrams from the English one.

    ./.venv/bin/python scripts/gen_hero_svg.py

`docs/images/where-your-data-comes-from.svg` is the source of truth for the
GEOMETRY; this script only substitutes strings, so a change to the drawing
propagates to all four languages instead of leaving three stale copies. Four
hand-maintained SVGs is exactly the arrangement that produces a diagram still
advertising a feature we removed.

**Why the width check exists.** SVG does not wrap or shrink text. A label that
outgrows its card silently overlaps the next shape, and CJK makes that the
default case rather than the exception: a CJK glyph advances a full em, so
`歩数 · 心拍数 · 睡眠` at font-size 9 is far wider than `steps · heart rate ·
sleep` at the same size looks. Every string below is checked against the box it
sits in, and the script refuses to write a file that would overflow.
"""

from __future__ import annotations

import pathlib
import re
import sys
import unicodedata

SRC = pathlib.Path(__file__).resolve().parent.parent / "docs" / "images" / "where-your-data-comes-from.svg"

#: A CJK/kana glyph advances one em; Latin averages far less. 0.55 is measured
#: against Helvetica's lowercase-heavy average and is deliberately pessimistic —
#: this check should fire early, not late.
def text_width(text: str, font_size: float) -> float:
    units = 0.0
    for ch in text:
        units += 1.0 if unicodedata.east_asian_width(ch) in "WF" else 0.55
    return units * font_size


#: english -> (font_size, budget_px, what the budget is)
#: Budgets come from the shapes in the SVG: source cards run x=24..240 with text
#: starting at x=78, so 162px; the FHIR panel is x=386..562; the AI card is
#: x=596..708; the answer card is x=806..944; the speech bubble is x=666..820.
BUDGETS: dict[str, tuple[float, float, str]] = {
    "Wearables":                            (12,  162, "source card title"),
    "steps · heart rate · sleep":           (9,   162, "source card subtitle"),
    "Phone health":                         (12,  162, "source card title"),
    "Apple Health · Health Connect":        (9,   162, "source card subtitle"),
    "Lab results":                          (12,  162, "source card title"),
    "blood tests · biomarkers":             (9,   162, "source card subtitle"),
    "Clinic records":                       (12,  162, "source card title"),
    "diagnoses · meds · visits":            (9,   162, "source card subtitle"),
    "Everyday logging":                     (12,  162, "source card title"),
    "food photos · voice mood · skin pics": (8.5, 162, "source card subtitle"),
    "YOUR DATA SOURCES":                    (11,  216, "column heading"),
    "normalizes everything to":             (11,  172, "FHIR panel"),
    "one standard format":                  (10,  172, "FHIR panel"),
    "AI model":                             (15,  108, "AI card"),
    "“Am I getting":                        (11,  150, "speech bubble line 1"),
    "healthier?”":                          (11,  150, "speech bubble line 2"),
    "Sleep up 12% this week":               (8.5, 134, "answer card headline"),
    "words + interactive charts · ECharts": (7.5, 134, "answer card caption"),
}

#: The two captions carry a <tspan>, so they are substituted in three pieces.
CAPTIONS = {
    "top": ("From wearables to food photos — ", "one standard format", ", ready for AI."),
    "bottom": ("Messy sources in — ", "one AI-ready format out.", ""),
}

FONTS = {
    "zh-CN": "Helvetica, Arial, 'PingFang SC', 'Hiragino Sans GB', 'Microsoft YaHei', 'Noto Sans CJK SC', sans-serif",
    "zh-TW": "Helvetica, Arial, 'PingFang TC', 'Microsoft JhengHei', 'Noto Sans CJK TC', sans-serif",
    "ja":    "Helvetica, Arial, 'Hiragino Sans', 'Yu Gothic', 'Noto Sans CJK JP', Meiryo, sans-serif",
}

STRINGS: dict[str, dict[str, str]] = {
    "zh-CN": {
        "YOUR DATA SOURCES":                    "你的数据来源",
        "Wearables":                            "穿戴设备",
        "steps · heart rate · sleep":           "步数 · 心率 · 睡眠",
        "Phone health":                         "手机健康数据",
        "Apple Health · Health Connect":        "Apple Health · Health Connect",
        "Lab results":                          "检验结果",
        "blood tests · biomarkers":             "血液检查 · 生物标志物",
        "Clinic records":                       "就诊记录",
        "diagnoses · meds · visits":            "诊断 · 用药 · 就诊",
        "Everyday logging":                     "日常记录",
        "food photos · voice mood · skin pics": "饭菜照片 · 语音心情 · 皮肤照片",
        "normalizes everything to":             "把一切标准化为",
        "one standard format":                  "一种标准格式",
        "AI model":                             "AI 模型",
        "OpenAI · Claude":                      "OpenAI · Claude",
        "Gemini · Qwen · DeepSeek":             "Gemini · Qwen · DeepSeek",
        "“Am I getting":                        "“我的身体",
        "healthier?”":                          "更健康了吗？”",
        "Sleep up 12% this week":               "本周睡眠提升 12%",
        "words + interactive charts · ECharts": "文字 + 交互图表 · ECharts",
        "mirobody":                             "mirobody",
        "FHIR R4":                              "FHIR R4",
        "__top__":    ("从穿戴设备到饭菜照片 —— ", "一种标准格式", "，AI 可直接读取。"),
        "__bottom__": ("杂乱的来源进 —— ", "一种 AI 可读的格式出。", ""),
    },
    "zh-TW": {
        "YOUR DATA SOURCES":                    "你的資料來源",
        "Wearables":                            "穿戴裝置",
        "steps · heart rate · sleep":           "步數 · 心率 · 睡眠",
        "Phone health":                         "手機健康資料",
        "Apple Health · Health Connect":        "Apple Health · Health Connect",
        "Lab results":                          "檢驗報告",
        "blood tests · biomarkers":             "血液檢查 · 生物標記",
        "Clinic records":                       "就醫紀錄",
        "diagnoses · meds · visits":            "診斷 · 用藥 · 就診",
        "Everyday logging":                     "日常紀錄",
        "food photos · voice mood · skin pics": "餐點照片 · 語音心情 · 皮膚照片",
        "normalizes everything to":             "全部標準化為",
        "one standard format":                  "一種標準格式",
        "AI model":                             "AI 模型",
        "OpenAI · Claude":                      "OpenAI · Claude",
        "Gemini · Qwen · DeepSeek":             "Gemini · Qwen · DeepSeek",
        "“Am I getting":                        "「我的身體",
        "healthier?”":                          "有變健康嗎？」",
        "Sleep up 12% this week":               "本週睡眠增加 12%",
        "words + interactive charts · ECharts": "文字 + 互動圖表 · ECharts",
        "mirobody":                             "mirobody",
        "FHIR R4":                              "FHIR R4",
        "__top__":    ("從穿戴裝置到餐點照片 —— ", "一種標準格式", "，AI 可直接讀取。"),
        "__bottom__": ("雜亂的來源進 —— ", "一種 AI 可讀的格式出。", ""),
    },
    "ja": {
        "YOUR DATA SOURCES":                    "データソース",
        "Wearables":                            "ウェアラブル",
        "steps · heart rate · sleep":           "歩数 · 心拍数 · 睡眠",
        "Phone health":                         "スマホの健康データ",
        "Apple Health · Health Connect":        "Apple Health · Health Connect",
        "Lab results":                          "検査結果",
        "blood tests · biomarkers":             "血液検査 · バイオマーカー",
        "Clinic records":                       "診療記録",
        "diagnoses · meds · visits":            "診断 · 処方 · 受診",
        "Everyday logging":                     "日々の記録",
        "food photos · voice mood · skin pics": "食事の写真 · 気分の音声 · 肌の写真",
        "normalizes everything to":             "すべてを標準化",
        "one standard format":                  "ひとつの標準形式",
        "AI model":                             "AI モデル",
        "OpenAI · Claude":                      "OpenAI · Claude",
        "Gemini · Qwen · DeepSeek":             "Gemini · Qwen · DeepSeek",
        "“Am I getting":                        "「体調は",
        "healthier?”":                          "よくなった？」",
        "Sleep up 12% this week":               "今週の睡眠 12% 増",
        "words + interactive charts · ECharts": "文章 + グラフ · ECharts",
        "mirobody":                             "mirobody",
        "FHIR R4":                              "FHIR R4",
        "__top__":    ("ウェアラブルから食事の写真まで —— ", "ひとつの標準形式", "に、AI が読める形で。"),
        "__bottom__": ("バラバラなデータを —— ", "AI が使える形式ひとつに。", ""),
    },
}


def check_widths(lang: str, strings: dict) -> list[str]:
    problems = []
    for english, (size, budget, where) in BUDGETS.items():
        translated = strings.get(english, english)
        w = text_width(translated, size)
        if w > budget:
            problems.append(
                f"  {lang}: {where} overflows by {w - budget:.0f}px — "
                f"{translated!r} is {w:.0f}px at font-size {size}, budget {budget}px"
            )
    return problems


def main() -> int:
    source = SRC.read_text(encoding="utf-8")
    failed = False

    for lang, strings in STRINGS.items():
        problems = check_widths(lang, strings)
        if problems:
            failed = True
            print(f"width check FAILED for {lang}:", file=sys.stderr)
            print("\n".join(problems), file=sys.stderr)
            continue

        out = source
        # Captions first: their English text also contains shorter keys.
        for slot, (prefix, span, suffix) in CAPTIONS.items():
            t_prefix, t_span, t_suffix = strings[f"__{slot}__"]
            out = out.replace(f">{prefix}<", f">{t_prefix}<", 1)
            out = out.replace(f">{span}</tspan>", f">{t_span}</tspan>", 1)
            if suffix:
                out = out.replace(f"</tspan>{suffix}<", f"</tspan>{t_suffix}<", 1)
            else:
                out = out.replace("</tspan></text>", f"</tspan>{t_suffix}</text>", 1)

        # Then the plain labels, longest first so no key is a prefix of another.
        for english in sorted(strings, key=len, reverse=True):
            if english.startswith("__"):
                continue
            out = out.replace(f">{english}</text>", f">{strings[english]}</text>")

        out = out.replace(
            'font-family="Helvetica, Arial, sans-serif"',
            f'font-family="{FONTS[lang]}"',
            1,
        )

        target = SRC.with_name(f"{SRC.stem}.{lang}.svg")
        target.write_text(out, encoding="utf-8")

        leftover = [
            m for m in re.findall(r">([^<>]{4,})</text>", out)
            if all(ord(c) < 0x2E80 for c in m) and m not in {
                # Deliberately not translated: brand and product names, and the
                # model list. Localizing "Apple Health" would be wrong in every
                # one of these languages — it is what the app is called.
                "mirobody", "FHIR R4", "OpenAI · Claude", "Gemini · Qwen · DeepSeek",
                "Apple Health · Health Connect",
            }
        ]
        print(f"wrote {target.name}  ({target.stat().st_size:,} bytes)"
              + (f"  UNTRANSLATED: {leftover}" if leftover else ""))

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
