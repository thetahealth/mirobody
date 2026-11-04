"""Finding the Health export inside an archive or directory by content.

An iPhone set to Chinese names the export `导出.xml`, not `export.xml`
(Discussion #233), so the lookup must not depend on the English name.
"""

import zipfile

import pytest

from mirobody.kernel.decoders import apple_export

HEALTH = b"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE HealthData [
<!ELEMENT HealthData (Record*)>
]>
<HealthData locale="zh_CN">
 <Record type="HKQuantityTypeIdentifierStepCount" sourceName="iPhone" unit="count"
  startDate="2026-10-01 08:00:00 +0800" endDate="2026-10-01 08:10:00 +0800" value="120"/>
</HealthData>
"""
CDA = b"""<?xml version="1.0" encoding="UTF-8"?>
<ClinicalDocument xmlns="urn:hl7-org:v3"/>
"""


def _kinds(path):
    return [kind for kind, _item in apple_export.iter_items(path)]


@pytest.mark.parametrize("name", ["export.xml", "导出.xml"])
def test_zip_export_is_found_by_content(tmp_path, name):
    archive = tmp_path / "export.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("apple_health_export/export_cda.xml", CDA)
        zf.writestr(f"apple_health_export/{name}", HEALTH)

    assert _kinds(archive) == ["HKQuantityTypeIdentifierStepCount"]


@pytest.mark.parametrize("name", ["export.xml", "导出.xml"])
def test_directory_export_is_found_by_content(tmp_path, name):
    folder = tmp_path / "apple_health_export"
    folder.mkdir()
    (folder / "export_cda.xml").write_bytes(CDA)
    (folder / name).write_bytes(HEALTH)

    assert _kinds(tmp_path) == ["HKQuantityTypeIdentifierStepCount"]


def test_zip_without_health_data_is_refused(tmp_path):
    archive = tmp_path / "export.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("apple_health_export/export_cda.xml", CDA)

    with pytest.raises(FileNotFoundError, match="no Apple Health export"):
        apple_export.open_export(archive)


def test_directory_without_health_data_is_refused(tmp_path):
    (tmp_path / "notes.xml").write_bytes(CDA)

    with pytest.raises(FileNotFoundError, match="no Apple Health export"):
        apple_export.open_export(tmp_path)
