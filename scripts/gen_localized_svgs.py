"""Regenerate the localized README diagrams from the English originals.

    ./.venv/bin/python scripts/gen_localized_svgs.py

The English `.svg` of each diagram is the source of truth for its GEOMETRY;
this script only substitutes strings, so a change to the drawing propagates to
all four languages instead of leaving three stale copies. Hand-maintained
translated SVGs are exactly the arrangement that ends with one of them still
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

IMAGES = pathlib.Path(__file__).resolve().parent.parent / "docs" / "images"

#: A CJK/kana glyph advances one em; Latin averages far less. 0.55 is measured
#: against Helvetica's lowercase-heavy average and is deliberately pessimistic —
#: this check should fire early, not late.
def text_width(text: str, font_size: float) -> float:
    units = 0.0
    for ch in text:
        units += 1.0 if unicodedata.east_asian_width(ch) in "WF" else 0.55
    return units * font_size


#: Per diagram: {english: (font_size, budget_px, which box)}.
#: Budgets are read off the shapes in the SVG, not guessed.
BUDGETS: dict[str, dict[str, tuple[float, float, str]]] = {
    # source cards run x=24..240 with text at x=78 -> 162px; FHIR panel
    # x=386..562; AI card x=596..708; answer card x=806..944; bubble x=666..820
    "where-your-data-comes-from": {
        "Wearables":                            (12,  162, "source card title"),
        "steps · heart rate · sleep":           (9,   162, "source card subtitle"),
        "Phone health":                         (12,  162, "source card title"),
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
        "\u201cAm I getting":                      (11,  150, "speech bubble line 1"),
        "healthier?\u201d":                        (11,  150, "speech bubble line 2"),
        "Sleep up 12% this week":               (8.5, 134, "answer card headline"),
        "words + interactive charts · ECharts": (7.5, 134, "answer card caption"),
    },
    # step rows have text at x=70 inside a column ending at x=258 -> 188px; the
    # control panel is x=24..258 with bullets at x=58 -> 200px; the three right
    # cards are x=662..940 with text at x=712/730/732; `access` is followed by a
    # pill at x=772, so it gets 42px and nothing more.
    "your-care-circle": {
        "Create a circle":                        (12,   188, "step title"),
        "name it — it's yours":                   (9,    188, "step subtitle"),
        "Invite by email":                        (12,   188, "step title"),
        "the people you trust":                   (9,    188, "step subtitle"),
        "They accept":                            (12,   188, "step title"),
        "acceptance required":                    (9,    188, "step subtitle"),
        "HOW IT WORKS":                           (11,   200, "column heading"),
        "You're in control":                      (11.5, 214, "control panel title"),
        "acceptance required to join":            (9,    196, "control panel bullet"),
        "remove a member anytime":                (9,    196, "control panel bullet"),
        "unshare a thread anytime":               (9,    196, "control panel bullet"),
        "health stays off until you allow it":    (9,    196, "control panel bullet"),
        "Care circle":                            (14,   200, "circle panel title"),
        "accepted members are mutually in the circle": (10, 340, "circle panel caption"),
        "WHAT YOU CAN SHARE":                     (11,   276, "column heading"),
        "Share a conversation":                   (12.5, 208, "share card title"),
        "it shows up in their history":           (9,    208, "share card subtitle"),
        "access":                                 (8.5,   40, "label before the two pills"),
        "View":                                   (8.5,   38, "permission pill"),
        "Edit":                                   (8.5,   38, "permission pill"),
        "Share health data":                      (12.5, 206, "share card title"),
        "your switch — off by default":           (9,    206, "share card subtitle"),
        "mutual — each member controls their own": (9,   206, "share card subtitle"),
        "Then the AI can answer":                 (12.5, 226, "answer card title"),
        "reads only what members chose to share":  (9,   226, "answer card caption"),
    },
}

#: Captions carry a <tspan>, so they are substituted in three pieces.
CAPTIONS: dict[str, dict[str, tuple[str, str, str]]] = {
    "where-your-data-comes-from": {
        "top": ("From wearables to food photos — ", "one standard format", ", ready for AI."),
        "bottom": ("Messy sources in — ", "one AI-ready format out.", ""),
    },
    "your-care-circle": {
        "top": ("Invite the people you trust — then ", "choose what to share.", ""),
        "bottom": ("Invite-only, acceptance required — ", "every share is your choice.", ""),
    },
}

FONTS = {
    "zh-CN": "Helvetica, Arial, 'PingFang SC', 'Hiragino Sans GB', 'Microsoft YaHei', 'Noto Sans CJK SC', sans-serif",
    "zh-TW": "Helvetica, Arial, 'PingFang TC', 'Microsoft JhengHei', 'Noto Sans CJK TC', sans-serif",
    "ja":    "Helvetica, Arial, 'Hiragino Sans', 'Yu Gothic', 'Noto Sans CJK JP', Meiryo, sans-serif",
}

#: Deliberately never translated: brand and product names, standards, model
#: lists. Localizing "Apple Health" would be wrong in all three languages.
KEEP_ENGLISH = {
    "mirobody", "FHIR R4", "OpenAI · Claude", "Gemini · Qwen · DeepSeek",
    "Apple Health · Health Connect",
}

STRINGS: dict[str, dict[str, dict[str, object]]] = {
    "where-your-data-comes-from": {
        "zh-CN": {
            "YOUR DATA SOURCES": "你的数据来源",
            "Wearables": "穿戴设备",
            "steps · heart rate · sleep": "步数 · 心率 · 睡眠",
            "Phone health": "手机健康数据",
            "Lab results": "检验结果",
            "blood tests · biomarkers": "血液检查 · 生物标志物",
            "Clinic records": "就诊记录",
            "diagnoses · meds · visits": "诊断 · 用药 · 就诊",
            "Everyday logging": "日常记录",
            "food photos · voice mood · skin pics": "饭菜照片 · 语音心情 · 皮肤照片",
            "normalizes everything to": "把一切标准化为",
            "one standard format": "一种标准格式",
            "AI model": "AI 模型",
            "\u201cAm I getting": "\u201c我的身体",
            "healthier?\u201d": "更健康了吗？\u201d",
            "Sleep up 12% this week": "本周睡眠提升 12%",
            "words + interactive charts · ECharts": "文字 + 交互图表 · ECharts",
            "__top__": ("从穿戴设备到饭菜照片 —— ", "一种标准格式", "，AI 可直接读取。"),
            "__bottom__": ("杂乱的来源进 —— ", "一种 AI 可读的格式出。", ""),
        },
        "zh-TW": {
            "YOUR DATA SOURCES": "你的資料來源",
            "Wearables": "穿戴裝置",
            "steps · heart rate · sleep": "步數 · 心率 · 睡眠",
            "Phone health": "手機健康資料",
            "Lab results": "檢驗報告",
            "blood tests · biomarkers": "血液檢查 · 生物標記",
            "Clinic records": "就醫紀錄",
            "diagnoses · meds · visits": "診斷 · 用藥 · 就診",
            "Everyday logging": "日常紀錄",
            "food photos · voice mood · skin pics": "餐點照片 · 語音心情 · 皮膚照片",
            "normalizes everything to": "全部標準化為",
            "one standard format": "一種標準格式",
            "AI model": "AI 模型",
            "\u201cAm I getting": "「我的身體",
            "healthier?\u201d": "有變健康嗎？」",
            "Sleep up 12% this week": "本週睡眠增加 12%",
            "words + interactive charts · ECharts": "文字 + 互動圖表 · ECharts",
            "__top__": ("從穿戴裝置到餐點照片 —— ", "一種標準格式", "，AI 可直接讀取。"),
            "__bottom__": ("雜亂的來源進 —— ", "一種 AI 可讀的格式出。", ""),
        },
        "ja": {
            "YOUR DATA SOURCES": "データソース",
            "Wearables": "ウェアラブル",
            "steps · heart rate · sleep": "歩数 · 心拍数 · 睡眠",
            "Phone health": "スマホの健康データ",
            "Lab results": "検査結果",
            "blood tests · biomarkers": "血液検査 · バイオマーカー",
            "Clinic records": "診療記録",
            "diagnoses · meds · visits": "診断 · 処方 · 受診",
            "Everyday logging": "日々の記録",
            "food photos · voice mood · skin pics": "食事の写真 · 気分の音声 · 肌の写真",
            "normalizes everything to": "すべてを標準化",
            "one standard format": "ひとつの標準形式",
            "AI model": "AI モデル",
            "\u201cAm I getting": "「体調は",
            "healthier?\u201d": "よくなった？」",
            "Sleep up 12% this week": "今週の睡眠 12% 増",
            "words + interactive charts · ECharts": "文章 + グラフ · ECharts",
            "__top__": ("ウェアラブルから食事の写真まで —— ", "ひとつの標準形式", "に、AI が読める形で。"),
            "__bottom__": ("バラバラなデータを —— ", "AI が使える形式ひとつに。", ""),
        },
    },
    "your-care-circle": {
        "zh-CN": {
            "HOW IT WORKS": "运作方式",
            "Create a circle": "创建一个圈子",
            "name it — it's yours": "自己命名，归你所有",
            "Invite by email": "用邮箱邀请",
            "the people you trust": "邀请你信任的人",
            "They accept": "对方接受",
            "acceptance required": "必须对方同意",
            "You're in control": "控制权在你手上",
            "acceptance required to join": "加入必须经对方同意",
            "remove a member anytime": "随时移除成员",
            "unshare a thread anytime": "随时取消分享某个对话",
            "health stays off until you allow it": "健康数据默认关闭，你允许才开启",
            "Care circle": "关爱圈",
            "you": "你",
            "mom": "妈妈",
            "dad": "爸爸",
            "accepted members are mutually in the circle": "接受邀请后，双方互相在圈内",
            "WHAT YOU CAN SHARE": "你可以分享什么",
            "Share a conversation": "分享一段对话",
            "it shows up in their history": "会出现在对方的历史记录里",
            "access": "权限",
            "View": "查看",
            "Edit": "编辑",
            "Share health data": "分享健康数据",
            "your switch — off by default": "由你开关 —— 默认关闭",
            "mutual — each member controls their own": "互相独立 —— 各管各的",
            "Then the AI can answer": "然后 AI 就能回答",
            "\u201cHow is my family doing?\u201d": "\u201c我家人的健康怎么样？\u201d",
            "reads only what members chose to share": "只读取成员选择分享的内容",
            "__top__": ("邀请你信任的人 —— ", "再决定分享什么。", ""),
            "__bottom__": ("仅限邀请、需对方接受 —— ", "每一次分享都由你决定。", ""),
        },
        "zh-TW": {
            "HOW IT WORKS": "運作方式",
            "Create a circle": "建立一個群組",
            "name it — it's yours": "自己命名，歸你所有",
            "Invite by email": "用 Email 邀請",
            "the people you trust": "邀請你信任的人",
            "They accept": "對方接受",
            "acceptance required": "必須對方同意",
            "You're in control": "控制權在你手上",
            "acceptance required to join": "加入必須經對方同意",
            "remove a member anytime": "隨時移除成員",
            "unshare a thread anytime": "隨時取消分享某個對話",
            "health stays off until you allow it": "健康資料預設關閉，你允許才開啟",
            "Care circle": "照護圈",
            "you": "你",
            "mom": "媽媽",
            "dad": "爸爸",
            "accepted members are mutually in the circle": "接受邀請後，雙方互相在圈內",
            "WHAT YOU CAN SHARE": "你可以分享什麼",
            "Share a conversation": "分享一段對話",
            "it shows up in their history": "會出現在對方的歷史紀錄裡",
            "access": "權限",
            "View": "檢視",
            "Edit": "編輯",
            "Share health data": "分享健康資料",
            "your switch — off by default": "由你開關 —— 預設關閉",
            "mutual — each member controls their own": "互相獨立 —— 各管各的",
            "Then the AI can answer": "然後 AI 就能回答",
            "\u201cHow is my family doing?\u201d": "「我家人的健康如何？」",
            "reads only what members chose to share": "只讀取成員選擇分享的內容",
            "__top__": ("邀請你信任的人 —— ", "再決定分享什麼。", ""),
            "__bottom__": ("僅限邀請、需對方接受 —— ", "每一次分享都由你決定。", ""),
        },
        "ja": {
            "HOW IT WORKS": "使い方",
            "Create a circle": "サークルを作る",
            "name it — it's yours": "名前は自分で決める",
            "Invite by email": "メールで招待",
            "the people you trust": "信頼できる人を",
            "They accept": "相手が承認",
            "acceptance required": "承認が必須",
            "You're in control": "決定権は本人にある",
            "acceptance required to join": "参加には承認が必要",
            "remove a member anytime": "メンバーはいつでも削除",
            "unshare a thread anytime": "スレッドの共有はいつでも解除",
            "health stays off until you allow it": "健康データは許可するまでオフ",
            "Care circle": "ケアサークル",
            "you": "本人",
            "mom": "母",
            "dad": "父",
            "accepted members are mutually in the circle": "承認したメンバーは相互にサークル内",
            "WHAT YOU CAN SHARE": "共有できるもの",
            "Share a conversation": "会話を共有",
            "it shows up in their history": "相手の履歴に表示される",
            "access": "権限",
            "View": "閲覧",
            "Edit": "編集",
            "Share health data": "健康データを共有",
            "your switch — off by default": "自分で切り替え — 既定はオフ",
            "mutual — each member controls their own": "相互 — 各自が自分の分を管理",
            "Then the AI can answer": "AI が答えられる",
            "\u201cHow is my family doing?\u201d": "「家族の調子はどう？」",
            "reads only what members chose to share": "共有を選んだ範囲だけを読む",
            "__top__": ("信頼できる人を招待して —— ", "共有する範囲を選ぶ。", ""),
            "__bottom__": ("招待制・承認が必須 —— ", "共有はすべて自分の判断で。", ""),
        },
    },
}


def check_widths(diagram: str, lang: str, strings: dict) -> list[str]:
    problems = []
    for english, (size, budget, where) in BUDGETS[diagram].items():
        translated = strings.get(english, english)
        if not isinstance(translated, str):
            continue
        w = text_width(translated, size)
        if w > budget:
            problems.append(
                f"  {diagram} [{lang}]: {where} overflows by {w - budget:.0f}px — "
                f"{translated!r} is {w:.0f}px at font-size {size}, budget {budget}px"
            )
    return problems


def main() -> int:
    failed = False

    for diagram, by_lang in STRINGS.items():
        source = (IMAGES / f"{diagram}.svg").read_text(encoding="utf-8")

        for lang, strings in by_lang.items():
            problems = check_widths(diagram, lang, strings)
            if problems:
                failed = True
                print("\n".join(problems), file=sys.stderr)
                continue

            out = source
            # Captions first: their English text contains shorter keys.
            for slot, (prefix, span, suffix) in CAPTIONS[diagram].items():
                t_prefix, t_span, t_suffix = strings[f"__{slot}__"]
                out = out.replace(f">{prefix}<", f">{t_prefix}<", 1)
                out = out.replace(f">{span}</tspan>", f">{t_span}</tspan>", 1)
                if suffix:
                    out = out.replace(f"</tspan>{suffix}<", f"</tspan>{t_suffix}<", 1)
                elif t_suffix:
                    out = out.replace("</tspan></text>", f"</tspan>{t_suffix}</text>", 1)

            # Then plain labels, longest first so no key is a prefix of another.
            for english in sorted((k for k in strings if not k.startswith("__")),
                                  key=len, reverse=True):
                out = out.replace(f">{english}</text>", f">{strings[english]}</text>")

            out = out.replace(
                'font-family="Helvetica, Arial, sans-serif"',
                f'font-family="{FONTS[lang]}"',
                1,
            )

            target = IMAGES / f"{diagram}.{lang}.svg"
            target.write_text(out, encoding="utf-8")

            leftover = [
                m for m in re.findall(r">([^<>]{4,})</text>", out)
                if all(ord(c) < 0x2E80 for c in m) and m not in KEEP_ENGLISH
            ]
            print(f"wrote {target.name}  ({target.stat().st_size:,} bytes)"
                  + (f"  UNTRANSLATED: {leftover}" if leftover else ""))

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
