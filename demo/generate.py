"""Rebuild the demo files in `upload/` from the panels written here.

    python demo/generate.py

Run it after changing a panel, or to give a new demo account its own files.
The seeded side (`seed/*.md`) is hand-written and not touched: its values have
to agree with `_PROFILES` in `mirobody/server/demo.py`, which is what
`tests/server/test_member_seed.py` checks.

Four formats on purpose. A health record arrives as whatever the lab, the
clinic and the family actually produce, so the demo uploads a PDF, a phone
photo, a spreadsheet and a CSV rather than four of the same thing. Needs
Pillow and openpyxl, both in the `[app]` extra, and the fonts macOS ships
(Arial and STHeiti) for the two photos.

Every file comes out the same bytes on every run: the panels and their dates
are literals, the PDFs are written out by hand, and the workbooks are saved
with a fixed timestamp (`_workbook_bytes`). The photos are the same bytes
for one version of Pillow and of the fonts.
"""

from __future__ import annotations

import csv
import io
import re
import zipfile
from itertools import groupby
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "upload"

BANNER = "SYNTHETIC SAMPLE - GENERATED DATA, NOT A REAL PATIENT RECORD"

#: The second panel each account gets, the one that does NOT come pre-seeded.
#: Every analyte here is spelled exactly as the seeded panel spells it, because
#: `collect.query` resolves the indicator NAME to a LOINC code at read time:
#: same name, same series, so an upload is a second point rather than a new
#: orphan indicator. Measured 2026-09-17: "Glycated Hemoglobin-HbA1c" resolves
#: to 4548-4 and "GlycatedHemoglobin-HbA1c" resolves to nothing.
PANELS = {
    "you_annual_checkup_2026-05": {
        "title": "Annual Checkup",
        "subject": "you@mirobody.ai / 41y",
        "collected": "2026-05-06",
        "exam": "annual physical (clinic)",
        "rows": [
            ("Glycated Hemoglobin-HbA1c", "5.2", "%", "4.0-5.6", ""),
            ("Fasting Blood Glucose-FBG", "4.9", "mmol/L", "3.9-6.1", ""),
            ("Total Cholesterol-TC", "4.45", "mmol/L", "3.0-5.18", ""),
            ("Low-Density Lipoprotein-LDL", "2.48", "mmol/L", "<3.4", ""),
            ("High-Density Lipoprotein-HDL", "1.50", "mmol/L", ">1.0", ""),
            ("Triglycerides-TG", "0.95", "mmol/L", "0.4-1.7", ""),
            ("Systolic Blood Pressure", "116", "mmHg", "<120", ""),
            ("Diastolic Blood Pressure", "75", "mmHg", "<80", ""),
            ("Resting Heart Rate", "57", "bpm", "60-100", "L"),
        ],
        "note": "No findings. Continue current activity level; recheck in 12 months.",
    },
    "mom_physical_2026-06": {
        "title": "Physical Examination",
        "subject": "mom@mirobody.ai / 63y",
        "collected": "2026-06-11",
        "exam": "follow-up (hospital)",
        "rows": [
            ("Glycated Hemoglobin-HbA1c", "6.0", "%", "4.0-5.6", "H"),
            ("Fasting Blood Glucose-FBG", "6.0", "mmol/L", "3.9-6.1", ""),
            ("Total Cholesterol-TC", "5.53", "mmol/L", "3.0-5.18", "H"),
            ("Low-Density Lipoprotein-LDL", "3.65", "mmol/L", "<3.4", "H"),
            ("High-Density Lipoprotein-HDL", "1.09", "mmol/L", ">1.0", ""),
            ("Triglycerides-TG", "1.90", "mmol/L", "0.4-1.7", "H"),
            ("Systolic Blood Pressure", "134", "mmHg", "<120", "H"),
            ("Diastolic Blood Pressure", "87", "mmHg", "<80", "H"),
            ("Resting Heart Rate", "73", "bpm", "60-100", ""),
        ],
        "note": "HbA1c and lipids above range; recheck in three months. Discussed activity and sleep.",
    },
}

#: A lipid panel a DIFFERENT lab exported as a csv, and it names the analytes
#: its own way. That is the point of this file: `Cholesterol, Total` and
#: `Total Cholesterol-TC` are the same test, and the record knows it because
#: both resolve to 14647-2. Measured 2026-09-17, six spellings and six matches.
#: The units match the other reports on purpose: LOINC puts the unit IN the
#: identity, so cholesterol is 14647-2 in mmol/L and 2093-3 in mg/dL, and
#: reconciling those two is the comparability key 1.5.0 brings.
#:
#: ONE collection date, like every file here. `resolve_report_date` gives a
#: whole file a single date, so a spreadsheet of seven mornings collapses to
#: one reading (measured: 7 rows in, 1 row out).
CSV_PANEL = {
    "collected": "2026-08-04",
    "rows": [
        ("Glucose, Fasting", "4.9", "mmol/L", "3.9-6.1"),
        ("Cholesterol, Total", "4.38", "mmol/L", "3.0-5.18"),
        ("LDL Cholesterol", "2.41", "mmol/L", "<3.4"),
        ("HDL Cholesterol", "1.52", "mmol/L", ">1.0"),
        ("Triglyceride", "0.91", "mmol/L", "0.4-1.7"),
    ],
}

#: What the clinic typed into a spreadsheet at mom's July visit.
#: Body weight and blood pressure are already in that account's device series
#: under the catalogue's names, so these land BESIDE them carrying the same
#: LOINC code: two sources, two rows, one code.
XLSX_PANEL = {
    "collected": "2026-07-07",
    "rows": [
        ("Body weight", 84.2, "kg"),
        ("Systolic Blood Pressure", 136, "mmHg"),
        ("Diastolic Blood Pressure", 88, "mmHg"),
        ("Fasting Blood Glucose-FBG", 6.2, "mmol/L"),
    ],
}


#: The same line on the reports printed in Chinese.
BANNER_ZH = "合成演示数据，非真实病历"

#: Three earlier years of `you`, from three places that share neither a
#: vocabulary nor a unit, which is what a decade of anyone's reports looks
#: like and what the four files above cannot show a reader of Chinese:
#:
#:   2023  a hospital lab slip, Chinese, analytes by abbreviation   LDL-C              mmol/L
#:   2024  a check-up centre's sheet, photographed, full names      低密度脂蛋白胆固醇  mg/dL
#:   2025  an international clinic's export, English                LDL Cholesterol    mg/dL
#:
#: Every row resolves, and each analyte lands on ONE series across the three
#: and across the files above (`demo/README.md` has the codes). Measured
#: 2026-11-12 against the shipped resolver: `LDL-C` in mmol/L is 22748-8 and
#: the two mg/dL spellings are 13457-7, one series, and 3.62 mmol/L, 128 and
#: 112 mg/dL read 3.62, 3.31 and 2.90 mmol/L. The names are chosen for that:
#: `FPG`, `空腹血糖` and `Glucose, Fasting` are fasting glucose in plasma,
#: where `GLU` and `葡萄糖(空腹)` resolve to glucose in whole blood, a
#: different series; and no urea row, because a Chinese lab prints urea and a
#: US one urea nitrogen, two analytes that must not share a line.
#:
#: The values fall from one year to the next toward the seeded 2025-11 panel
#: and the 2026 uploads (cholesterol 5.54, 5.25, 4.84, then 4.60, 4.45, 4.38
#: mmol/L), so the six files of this account draw one line. Total cholesterol
#: agrees with the other three lipids (Friedewald) on every report.
#:
#: The hospital, the people and every number on them are invented.
HISTORY = {
    "you_lab_report_zh_2023-03.pdf": {
        "institution": "青禾市第一人民医院",
        "title": "检验报告单",
        "collected": "2023-03-21",
        "fields": (
            ("姓名：张三", "性别：男", "年龄：38岁", "病历号：SYN0000417"),
            ("科别：体检科", "标本：静脉血", "申请医生：李四", "条码号：SYN2303210417"),
            ("采集时间：2023-03-21 07:52", "", "报告时间：2023-03-21 14:36", ""),
        ),
        "rows": [
            ("WBC", "6.12", "10^9/L", "3.5-9.5", ""),
            ("RBC", "4.86", "10^12/L", "4.3-5.8", ""),
            ("HGB", "149", "g/L", "130-175", ""),
            ("PLT", "238", "10^9/L", "125-350", ""),
            ("ALT", "34", "U/L", "9-50", ""),
            ("AST", "27", "U/L", "15-40", ""),
            ("TBIL", "13.6", "µmol/L", "0-23", ""),
            ("ALB", "46.2", "g/L", "40-55", ""),
            ("CREA", "84", "µmol/L", "57-97", ""),
            ("UA", "396", "µmol/L", "208-428", ""),
            ("FPG", "5.4", "mmol/L", "3.9-6.1", ""),
            ("HbA1c", "5.5", "%", "4.0-6.0", ""),
            ("TC", "5.54", "mmol/L", "<5.18", "↑"),
            ("TG", "1.62", "mmol/L", "<1.70", ""),
            ("HDL-C", "1.18", "mmol/L", ">1.04", ""),
            ("LDL-C", "3.62", "mmol/L", "<3.37", "↑"),
        ],
        "signed": ("检验者：王五", "审核者：赵六"),
        "note": "本报告仅对所检标本负责。医院、姓名、编号均为虚构。",
    },
    "you_checkup_photo_zh_2024-04.jpg": {
        "institution": "南汀健康体检中心",
        "title": "健康体检报告（检验部分）",
        "collected": "2024-04-16",
        "fields": (
            "姓名：张三　　性别：男　　年龄：39岁　　体检编号：SYN240416007",
            # One day for both, as on the slip above. A sheet that prints two
            # is filed under whichever the model reads as "the examination
            # date": measured 2026-11-12 on 1.6.1 with the 2B local model, a
            # report date of the 17th was chosen over a sampling date of the 16th.
            "采样日期：2024-04-16　　报告日期：2024-04-16",
        ),
        "sections": (("血常规", 4), ("肝功能", 4), ("肾功能", 2), ("血糖", 2), ("血脂", 4)),
        "rows": [
            ("白细胞计数", "5.87", "10^9/L", "3.5-9.5", ""),
            ("红细胞计数", "4.91", "10^12/L", "4.3-5.8", ""),
            ("血红蛋白", "15.1", "g/dL", "13.0-17.5", ""),
            ("血小板计数", "226", "10^9/L", "125-350", ""),
            ("丙氨酸氨基转移酶", "29", "U/L", "9-50", ""),
            ("天门冬氨酸氨基转移酶", "25", "U/L", "15-40", ""),
            ("总胆红素", "0.8", "mg/dL", "0.2-1.2", ""),
            ("白蛋白", "4.5", "g/dL", "4.0-5.5", ""),
            ("肌酐", "0.95", "mg/dL", "0.64-1.10", ""),
            ("尿酸", "6.3", "mg/dL", "3.5-7.2", ""),
            ("空腹血糖", "95", "mg/dL", "70-110", ""),
            ("糖化血红蛋白", "5.4", "%", "4.0-6.0", ""),
            ("总胆固醇", "203", "mg/dL", "<200", "↑"),
            ("甘油三酯", "128", "mg/dL", "<150", ""),
            ("高密度脂蛋白胆固醇", "49", "mg/dL", ">40", ""),
            ("低密度脂蛋白胆固醇", "128", "mg/dL", "<130", ""),
        ],
        "note": "小结：总胆固醇略高于参考范围，建议低脂饮食、规律运动，一年后复查。",
        "disclaimer": "本文件为合成演示数据：机构、姓名、编号均为虚构。",
    },
    "you_lab_results_en_2025-03.xlsx": {
        "institution": "Lumen Bay International Clinic",
        "collected": "2025-03-25",
        "sections": (("Complete Blood Count", 4), ("Liver Panel", 4), ("Kidney Panel", 2),
                     ("Glucose", 2), ("Lipid Panel", 4)),
        "rows": [
            ("White Blood Cell Count", "5.6", "10^3/uL", "3.5-9.5", ""),
            ("Red Blood Cell Count", "4.88", "10^6/uL", "4.30-5.80", ""),
            ("Hemoglobin", "14.8", "g/dL", "13.0-17.5", ""),
            ("Platelet Count", "241", "10^3/uL", "125-350", ""),
            ("ALT (SGPT)", "24", "U/L", "9-50", ""),
            ("AST (SGOT)", "22", "U/L", "15-40", ""),
            ("Bilirubin, Total", "0.7", "mg/dL", "0.2-1.2", ""),
            ("Albumin", "4.6", "g/dL", "4.0-5.5", ""),
            ("Creatinine", "0.93", "mg/dL", "0.64-1.10", ""),
            ("Uric Acid", "5.8", "mg/dL", "3.5-7.2", ""),
            ("Glucose, Fasting", "92", "mg/dL", "70-99", ""),
            ("Hemoglobin A1c", "5.3", "%", "4.0-5.6", ""),
            ("Cholesterol, Total", "187", "mg/dL", "<200", ""),
            ("Triglycerides", "112", "mg/dL", "<150", ""),
            ("HDL Cholesterol", "53", "mg/dL", ">40", ""),
            ("LDL Cholesterol", "112", "mg/dL", "<130", ""),
        ],
        "about": (
            BANNER,
            "Lumen Bay International Clinic, patient portal export",
            "Patient: ZHANG San (M, 40y), record SYN-250325-0193",
            "The clinic, the person and every number in this file are invented.",
        ),
    },
}


# ── PDF ──────────────────────────────────────────────────────────────────────
# Written out by hand rather than with a PDF library: a demo fixture is not
# worth a dependency, and the whole file is seven objects of Helvetica text.

def _escape(text: str) -> str:
    return text.replace("\\", r"\\").replace("(", r"\(").replace(")", r"\)")


def _text(x: float, y: float, body: str, *, size: int = 10, bold: bool = False) -> str:
    font = "F2" if bold else "F1"
    return f"BT /{font} {size} Tf 1 0 0 1 {x} {y} Tm ({_escape(body)}) Tj ET\n"


def _pdf_stream(panel: dict) -> str:
    out = _text(48, 794, BANNER, size=9, bold=True)
    out += _text(48, 770, panel["title"], size=18, bold=True)
    out += _text(48, 747.5, f"Subject:   {panel['subject']}")
    out += _text(48, 732.5, f"Collected: {panel['collected']}")
    out += _text(48, 717.5, f"Exam:      {panel['exam']}")

    columns = ((48, "Analyte"), (298, "Result"), (368, "Unit"), (438, "Reference"), (528, "Flag"))
    for x, label in columns:
        out += _text(x, 690.5, label, bold=True)
    out += "0.5 w 48 684.5 m 547 684.5 l S\n"

    y = 669.5
    for analyte, result, unit, reference, flag in panel["rows"]:
        out += _text(48, y, analyte)
        out += _text(298, y, result, bold=True)
        out += _text(368, y, unit)
        out += _text(438, y, reference)
        if flag:
            out += _text(528, y, flag)
        y -= 15

    out += _text(48, y - 15, "Note: " + panel["note"])
    return out


def _pdf(panel: dict) -> bytes:
    return _pdf_file(_pdf_stream(panel), f"({_escape(panel['title'])})")


def _pdf_file(stream: str, title: str, *, cjk: bool = False) -> bytes:
    """One A4 page drawing `stream`. `title` is a PDF string with its
    delimiters; `cjk` adds the font `_mixed` sets Chinese in."""
    objects = [
        "<< /Type /Catalog /Pages 2 0 R >>",
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources "
        f"<< /Font << /F1 4 0 R /F2 5 0 R{' /F3 7 0 R' if cjk else ''} >> >> /Contents 6 0 R >>",
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>",
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold /Encoding /WinAnsiEncoding >>",
        f"<< /Length {len(stream)} >>\nstream\n{stream}endstream",
    ]
    if cjk:
        objects += _CJK_FONT

    body = "%PDF-1.4\n"
    offsets = []
    for number, obj in enumerate(objects, start=1):
        offsets.append(len(body))
        body += f"{number} 0 obj\n{obj}\nendobj\n"

    xref_at = len(body)
    body += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n"
    body += "".join(f"{offset:010d} 00000 n \n" for offset in offsets)
    body += (
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R /Info "
        f"<< /Title {title} /Producer (mirobody demo) >> >>\n"
        f"startxref\n{xref_at}\n%%EOF\n"
    )
    return body.encode("latin-1")


# ── PDF, in Chinese ──────────────────────────────────────────────────────────
# The same seven hand-written objects and three more for a font. The Chinese
# is set in STSong-Light, one of the fonts every PDF reader maps to a Song
# face of its own, so nothing is embedded: no font file to license, and the
# page is still a few kilobytes. Its text layer is real (the predefined
# UniGB-UCS2-H encoding is UCS-2, so a reader gets every character back),
# which is the path a born-digital hospital report takes through
# `documents.extract`: read from the layer, never sent to a vision model.

_CJK_FONT = [
    "<< /Type /Font /Subtype /Type0 /BaseFont /STSong-Light /Encoding /UniGB-UCS2-H "
    "/DescendantFonts [8 0 R] >>",
    "<< /Type /Font /Subtype /CIDFontType0 /BaseFont /STSong-Light /CIDSystemInfo "
    "<< /Registry (Adobe) /Ordering (GB1) /Supplement 2 >> /FontDescriptor 9 0 R /DW 1000 >>",
    "<< /Type /FontDescriptor /FontName /STSong-Light /Flags 6 /FontBBox [-25 -254 1000 880] "
    "/ItalicAngle 0 /Ascent 752 /Descent -271 /CapHeight 737 /StemV 58 >>",
]


def _ucs2(text: str) -> str:
    return "<" + text.encode("utf-16-be").hex().upper() + ">"


def _mixed(x: float, y: float, body: str, *, size: int = 10, bold: bool = False) -> str:
    """One line in two fonts: Chinese in STSong-Light, everything Latin-1 in
    Helvetica. The Latin letters of an unembedded Song face have no widths a
    reader agrees on, so `LDL-C` and `mmol/L` set in it come out spaced like
    ideographs. A reader advances by each font's own widths, so the runs
    follow one another with no measuring here. STSong has one weight: bold is
    the same glyph filled and stroked."""
    out = f"BT 1 0 0 1 {x} {y} Tm "
    for chinese, run in groupby(body, key=lambda ch: ord(ch) > 0xFF):
        text = "".join(run)
        if not chinese:
            out += f"/{'F2' if bold else 'F1'} {size} Tf ({_escape(text)}) Tj "
        elif bold:
            out += f"/F3 {size} Tf 0.35 w 2 Tr {_ucs2(text)} Tj 0 Tr "
        else:
            out += f"/F3 {size} Tf {_ucs2(text)} Tj "
    return out + "ET\n"


def _centred(y: float, body: str, *, size: int) -> str:
    """`body` in the middle of the page. Chinese only: an ideograph is as wide
    as the font size, which is all the measuring this needs."""
    return _mixed((595 - len(body) * size) / 2, y, body, size=size, bold=True)


def _pdf_zh_stream(panel: dict) -> str:
    out = _mixed(48, 806, BANNER_ZH, size=9, bold=True)
    out += _mixed(48, 794, BANNER, size=9, bold=True)
    out += _centred(764, panel["institution"], size=16)
    out += _centred(742, panel["title"], size=13)

    # Four fields to a line, as a hospital's slip prints them. The last line
    # before the table is one field alone, so the reader of the text layer
    # sees where the patient's details end and the table begins.
    y = 712
    for line in panel["fields"]:
        for x, field in zip((48, 178, 308, 418), line, strict=True):
            if field:
                out += _mixed(x, y, field)
        y -= 16
    out += _mixed(48, y, "临床诊断：健康体检")

    y -= 30
    columns = ((48, "项目"), (218, "结果"), (288, "提示"), (338, "单位"), (438, "参考区间"))
    for x, label in columns:
        out += _mixed(x, y, label, bold=True)
    out += f"0.5 w 48 {y - 6} m 547 {y - 6} l S\n"

    y -= 22
    for analyte, result, unit, reference, flag in panel["rows"]:
        out += _mixed(48, y, analyte)
        out += _mixed(218, y, result, bold=True)
        if flag:
            out += _mixed(288, y, flag)
        out += _mixed(338, y, unit)
        out += _mixed(438, y, reference)
        y -= 17
    out += f"0.5 w 48 {y + 9} m 547 {y + 9} l S\n"

    out += _mixed(48, y - 10, "备注：" + panel["note"])
    for x, field in zip((48, 178), panel["signed"], strict=True):
        out += _mixed(x, y - 28, field)
    return out


def _pdf_zh(panel: dict) -> bytes:
    title = "FEFF" + f"{panel['institution']} {panel['title']}".encode("utf-16-be").hex().upper()
    return _pdf_file(_pdf_zh_stream(panel), f"<{title}>", cjk=True)


# ── JPG ──────────────────────────────────────────────────────────────────────
# A phone photo of a printed slip, because that is how most people hand over a
# report. Slightly rotated and slightly uneven in brightness: a fixture that is
# a clean screenshot would not exercise the path the picture actually takes.

def _jpg(panel: dict) -> bytes:
    from PIL import Image, ImageDraw, ImageFont

    def font(size: int, bold: bool = False):
        name = "Arial Bold.ttf" if bold else "Arial.ttf"
        return ImageFont.truetype(f"/System/Library/Fonts/Supplemental/{name}", size)

    page = Image.new("RGB", (1240, 1754), "white")
    draw = ImageDraw.Draw(page)
    draw.text((70, 60), BANNER, font=font(22, bold=True), fill=(120, 120, 120))
    draw.text((70, 110), panel["title"], font=font(46, bold=True), fill="black")
    draw.text((70, 185), f"Subject:   {panel['subject']}", font=font(26), fill="black")
    draw.text((70, 225), f"Collected: {panel['collected']}", font=font(26), fill="black")
    draw.text((70, 265), f"Exam:      {panel['exam']}", font=font(26), fill="black")

    columns = ((70, "Analyte"), (640, "Result"), (800, "Unit"), (960, "Reference"), (1130, "Flag"))
    for x, label in columns:
        draw.text((x, 340), label, font=font(26, bold=True), fill="black")
    draw.line((70, 380, 1170, 380), fill="black", width=2)

    y = 405
    for analyte, result, unit, reference, flag in panel["rows"]:
        draw.text((70, y), analyte, font=font(26), fill="black")
        draw.text((640, y), result, font=font(26, bold=True), fill="black")
        draw.text((800, y), unit, font=font(26), fill="black")
        draw.text((960, y), reference, font=font(26), fill="black")
        draw.text((1130, y), flag, font=font(26), fill="black")
        y += 46

    draw.text((70, y + 40), "Note: " + panel["note"], font=font(24), fill="black")
    page = page.crop((0, 0, page.width, y + 140))

    # Photographed, not scanned: a slight rotation and one end that caught less
    # light. Enough to look like a picture, not enough to hurt legibility.
    page = page.rotate(-1.1, resample=Image.BICUBIC, expand=True, fillcolor=(238, 236, 232))
    shade = Image.linear_gradient("L").resize(page.size).point(lambda v: 150 + v // 3)
    page = Image.composite(page, Image.new("RGB", page.size, (0, 0, 0)), shade)

    buffer = io.BytesIO()
    page.resize((page.width // 2, page.height // 2)).save(buffer, "JPEG", quality=82)
    return buffer.getvalue()


# ── JPG, in Chinese ──────────────────────────────────────────────────────────
# A check-up centre's printed sheet on a desk, photographed by its owner:
# panels under their headings, the page a little turned, the desk showing
# round it, and the corner nearest the phone in its shadow. Twice the pixels
# of the photo above, because a Chinese character has four times the strokes
# of a Latin letter and has to survive the same read by a vision model.

def _photo_zh(panel: dict) -> bytes:
    from PIL import Image, ImageChops, ImageDraw, ImageFilter, ImageFont

    def font(size: int, bold: bool = False):
        name = "STHeiti Medium.ttc" if bold else "STHeiti Light.ttc"
        return ImageFont.truetype(f"/System/Library/Fonts/{name}", size)

    ink, grey, paper, desk = (28, 28, 30), (110, 110, 110), (252, 251, 247), (64, 56, 48)
    width, left, right = 1500, 96, 1404
    page = Image.new("RGB", (width, 2400), paper)
    draw = ImageDraw.Draw(page)

    draw.text((left, 64), f"{BANNER_ZH}  {BANNER}", font=font(23), fill=grey)
    for y, text, size in ((122, panel["institution"], 56), (204, panel["title"], 38)):
        face = font(size, bold=True)
        draw.text(((width - draw.textlength(text, font=face)) / 2, y), text, font=face, fill=ink)
    y = 290
    for line in panel["fields"]:
        draw.text((left, y), line, font=font(30), fill=ink)
        y += 48

    y += 22
    columns = ((left, "检查项目"), (640, "结果"), (810, "单位"), (1010, "参考范围"), (1280, "提示"))
    draw.line((left, y, right, y), fill=ink, width=3)
    for x, label in columns:
        draw.text((x, y + 16), label, font=font(31, bold=True), fill=ink)
    y += 68
    draw.line((left, y, right, y), fill=ink, width=2)

    rows = iter(panel["rows"])
    for heading, count in panel["sections"]:
        y += 16
        draw.text((left, y), f"【{heading}】", font=font(31, bold=True), fill=ink)
        y += 54
        for _ in range(count):
            analyte, result, unit, reference, flag = next(rows)
            draw.text((left + 24, y), analyte, font=font(31), fill=ink)
            draw.text((640, y), result, font=font(31, bold=True), fill=ink)
            draw.text((810, y), unit, font=font(31), fill=ink)
            draw.text((1010, y), reference, font=font(31), fill=ink)
            draw.text((1280, y), flag, font=font(31, bold=True), fill=ink)
            y += 54
    y += 10
    draw.line((left, y, right, y), fill=ink, width=3)

    draw.text((left, y + 34), panel["note"], font=font(29), fill=ink)
    draw.text((left, y + 86), panel["disclaimer"], font=font(25), fill=grey)
    page = page.crop((0, 0, width, y + 170))

    # The sheet on the desk, turned the way a hand holds a phone over it.
    photo = Image.new("RGB", (page.width + 150, page.height + 150), desk)
    photo.paste(page, (75, 75))
    photo = photo.rotate(1.7, resample=Image.BICUBIC, fillcolor=desk)

    # Light from the far side of the desk, and the phone's shadow over the
    # near left corner. Soft enough that every character stays dark on light.
    w, h = photo.size
    light = Image.linear_gradient("L").resize((w, h)).point(lambda v: 255 - v // 5)
    shadow = Image.new("L", (w, h), 255)
    ImageDraw.Draw(shadow).polygon([(0, int(h * 0.52)), (int(w * 0.56), h), (0, h)], fill=176)
    shadow = shadow.filter(ImageFilter.GaussianBlur(110))
    shade = ImageChops.multiply(light, shadow)
    photo = ImageChops.multiply(photo, Image.merge("RGB", (shade, shade, shade)))
    photo = photo.filter(ImageFilter.GaussianBlur(0.5))

    buffer = io.BytesIO()
    photo.save(buffer, "JPEG", quality=80)
    return buffer.getvalue()


# ── CSV and XLSX ─────────────────────────────────────────────────────────────

def _csv() -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["collected", "analyte", "result", "unit", "reference"])
    for analyte, result, unit, reference in CSV_PANEL["rows"]:
        writer.writerow([CSV_PANEL["collected"], analyte, result, unit, reference])
    return buffer.getvalue().encode("utf-8")


def _xlsx() -> bytes:
    from openpyxl import Workbook

    book = Workbook()
    sheet = book.active
    sheet.title = "Clinic visit"
    # Spelled out, not "Systolic"/"Diastolic". Measured 2026-09-17 against the
    # shipped resolver: the bare words send systolic to 24372-5 (peak pressure
    # during a rising phase) and leave diastolic unresolved, while the full
    # names land on 8480-6 and 8462-4.
    sheet.append(["Date", "Measurement", "Result", "Unit"])
    for measurement, result, unit in XLSX_PANEL["rows"]:
        sheet.append([XLSX_PANEL["collected"], measurement, result, unit])
    for column, width in zip("ABCD", (14, 30, 12, 12)):
        sheet.column_dimensions[column].width = width
    return _workbook_bytes(book)


def _xlsx_history(panel: dict) -> bytes:
    """A clinic's patient portal export: one row per test, the collection
    date and the panel on every row, results as numbers. The first row is
    the header, because that is what `documents.extract` reads a sheet's
    first row as; what a slip prints above its table goes on a second sheet."""
    from openpyxl import Workbook
    from openpyxl.styles import Font

    book = Workbook()
    sheet = book.active
    sheet.title = "Lab results (synthetic sample)"
    sheet.append(["Collected", "Panel", "Test", "Result", "Units", "Reference Range", "Flag"])
    rows = iter(panel["rows"])
    for heading, count in panel["sections"]:
        for _ in range(count):
            test, result, unit, reference, flag = next(rows)
            number = float(result) if "." in result else int(result)
            sheet.append([panel["collected"], heading, test, number, unit, reference, flag])
    for cell in sheet[1]:
        cell.font = Font(bold=True)
    sheet.freeze_panes = "A2"
    for column, width in zip("ABCDEFG", (13, 24, 26, 10, 11, 17, 7), strict=True):
        sheet.column_dimensions[column].width = width

    about = book.create_sheet("About this file")
    for line in panel["about"]:
        about.append([line])
    about.column_dimensions["A"].width = 72
    return _workbook_bytes(book)


#: The moment every workbook says it was written. openpyxl stamps a workbook
#: with the clock as it saves, and the zip around it stamps every member, so
#: two runs of this script gave two different files for the same cells.
_WORKBOOK_STAMP = b"2026-01-01T00:00:00Z"
_WORKBOOK_CLOCK = re.compile(rb"(<dcterms:(created|modified)[^>]*>)[^<]*(</dcterms:\2>)")


def _workbook_bytes(book) -> bytes:
    """`book` as an .xlsx whose bytes depend on its cells alone: both dates in
    its properties are the stamp, every member carries the zip format's
    earliest date, and the members are stored, not deflated, so the zlib a
    Python was built with does not show in the file either."""
    saved = io.BytesIO()
    book.save(saved)
    packed = io.BytesIO()
    with zipfile.ZipFile(saved) as source, zipfile.ZipFile(packed, "w", zipfile.ZIP_STORED) as target:
        for member in source.infolist():
            content = source.read(member)
            if member.filename == "docProps/core.xml":
                content = _WORKBOOK_CLOCK.sub(rb"\g<1>" + _WORKBOOK_STAMP + rb"\g<3>", content)
            entry = zipfile.ZipInfo(member.filename, date_time=(1980, 1, 1, 0, 0, 0))
            entry.external_attr = 0o644 << 16
            target.writestr(entry, content)
    return packed.getvalue()


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    written = {
        "you_annual_checkup_2026-05.pdf": _pdf(PANELS["you_annual_checkup_2026-05"]),
        "you_lipid_panel_2026-08.csv": _csv(),
        "mom_physical_2026-06.jpg": _jpg(PANELS["mom_physical_2026-06"]),
        "mom_clinic_visit_2026-07.xlsx": _xlsx(),
    }
    writers = {".pdf": _pdf_zh, ".jpg": _photo_zh, ".xlsx": _xlsx_history}
    for name, panel in HISTORY.items():
        written[name] = writers[Path(name).suffix](panel)
    for name, payload in written.items():
        (OUT / name).write_bytes(payload)
        print(f"{name:40} {len(payload):>8,} bytes")


if __name__ == "__main__":
    main()
