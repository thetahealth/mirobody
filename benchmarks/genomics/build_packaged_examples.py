"""Render small, public genotype examples into mirobody/testing/genomics.

HG00096 calls come from pinned 1000 Genomes phase 3 region extracts. The
single no-call comes from an openly shared PGP 23andMe export. No variant or
genotype value is fabricated; each selected site is checked against the
packaged dbSNP candidate before a vendor rendering is written.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import re
import sqlite3
import struct
import zipfile
import zlib
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
CATALOG = ROOT / "mirobody/res/genomics/genotype_sites.sqlite3"
X_SOURCE = HERE / "fixtures/public-1000g-x.tsv"
CATALOG_SHA256 = "557fed0f94ff613a091be21a877ebc2b4f7dd2e5019ebfcb6d636afd84083b60"
X_SHA256 = "70e3a0c535b472b3fbee5f1ae23451f68cab38927126e58ecd8912e3a700f1cf"
PGP_SHA256 = "515559e5019e1557a3ea18b0a19ac88ea394f6504ef918dada44e58bb168d506"
PGP_URL = "https://my.pgp-hms.org/user_file/download/4220"
REGION_URL = "https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/"
X_URL = (REGION_URL + "ALL.chrX.phase3_shapeit2_mvncall_integrated_v1c.20130502.genotypes.vcf.gz")
REGIONS = {
    "CYP2B6": ("rs3745274",),
    "CYP2C19": ("rs4986893", "rs4244285"),
    "CYP2C9": ("rs1057910",),
    "CYP3A5": ("rs10264272",),
    "CYP4F2": ("rs3093153",),
    "DPYD": ("rs1801159",),
    "NAT2": ("rs1801280",),
    "NUDT15": ("rs116855232",),
    "SLCO1B1": ("rs4149056",),
    "TPMT": ("rs1142345",),
    "UGT1A1": ("rs887829",),
    "VKORC1": ("rs9923231",),
}
REGION_SHA256 = {
    "CYP2B6": "373688178ac0539915a59261f43133b4e8dd42fb142316d5140544f24bec55d0",
    "CYP2C19": "c63f2e17f9fa7ed06d75c0c03824233eced60910d2a2e5a04cb7b51cab0921ca",
    "CYP2C9": "b95b6d71588c7985dbd54ae878948a8e40217772033bbdc71fd606ea1b634439",
    "CYP3A5": "8e716b2f8307ca10853773f7b08ce2fb54f1f48f8f834caf7c7239a16daaff3d",
    "CYP4F2": "e8c7e69029c74485ef36972c25ad8eaba6c0de106b1f75f9250e5eaaab382059",
    "DPYD": "0bd37d6b30a21259a1a6840b93de399c6d484c1000609171caf05c9185d038ca",
    "NAT2": "5304567be8951bbeb44bc57359bfd1590230e10aa55edbc3b344d74e67960814",
    "NUDT15": "defd0ef2f69183bd5130b24f2bf61ed01333f004dbabf40d465b619413b6e4bb",
    "SLCO1B1": "4b68e0dd9e57a894a92c3b63ed8d632680fa09370579a5bd35ecd01866a92a8d",
    "TPMT": "b0986a20a4969b0102fb25e09a64157866892899b21ef7570486490ad9a159e1",
    "UGT1A1": "94745960406de7299df7d3a61d539e048b16562fcef7dbf71fe17a8938abd328",
    "VKORC1": "4ef02c2d7809555edd1b4a8300a85aaba3fe529341c67f1283fedec1584ca768",
}


@dataclass(frozen=True)
class Call:
    rsid: str
    gene: str
    chrom: str
    pos37: int
    pos38: int
    ref: str
    source_alt: tuple[str, ...]
    catalog_alt: tuple[str, ...]
    source_gt: str

    @property
    def bases(self) -> tuple[str, ...]:
        alleles = (self.ref, *self.source_alt)
        return tuple(alleles[int(index)] for index in re.split(r"[/|]", self.source_gt))

    @property
    def array_gt(self) -> str:
        alleles = (self.ref, *self.catalog_alt)
        return "/".join(str(index) for index in sorted(alleles.index(base) for base in self.bases))

    @property
    def vcf_gt(self) -> str:
        alleles = (self.ref, *self.source_alt)
        catalog = (self.ref, *self.catalog_alt)
        separator = "|" if "|" in self.source_gt else "/"
        return separator.join(str(catalog.index(alleles[int(index)]))
                              for index in re.split(r"[/|]", self.source_gt))


def digest(path: Path) -> str:
    sha = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            sha.update(block)
    return sha.hexdigest()


def _require_hash(path: Path, expected: str) -> None:
    if digest(path) != expected:
        raise ValueError(f"public source hash mismatch: {path.name}")


def _region_calls(directory: Path) -> list[Call]:
    _require_hash(CATALOG, CATALOG_SHA256)
    selected = []
    with sqlite3.connect(CATALOG) as db:
        for gene, rsids in REGIONS.items():
            sites = {}
            for rsid in rsids:
                row = db.execute(
                    "SELECT chrom,pos37,pos38,ref,alt,gene FROM sites WHERE rsid=?", (rsid,),
                ).fetchone()
                if row is None or row[5] != gene:
                    raise ValueError(f"candidate site or gene changed: {rsid}")
                sites[(row[0], row[1])] = (rsid, row)
            path = directory / f"{gene}.GRCh37.vcf.gz"
            _require_hash(path, REGION_SHA256[gene])
            found = {}
            sample_index = None
            with gzip.open(path, "rt", encoding="utf-8") as stream:
                for line in stream:
                    if line.startswith("#CHROM"):
                        sample_index = line.rstrip("\r\n").split("\t").index("HG00096")
                    elif not line.startswith("#"):
                        fields = line.rstrip("\r\n").split("\t")
                        match = sites.get((fields[0], int(fields[1])))
                        if match is None:
                            continue
                        rsid, row = match
                        alts = tuple(fields[4].split(","))
                        gt = fields[sample_index].split(":", 1)[0]
                        if (fields[3] != row[3] or not set(alts) <= set(row[4].split(","))
                                or not re.fullmatch(r"[0-9]+(?:[|/][0-9]+)", gt)
                                or any(int(index) > len(alts) for index in re.split(r"[/|]", gt))
                                or rsid in found):
                            raise ValueError(f"ambiguous public call or REF/ALT disagreement: {rsid}")
                        found[rsid] = Call(rsid, gene, row[0], row[1], row[2], row[3],
                                           alts, tuple(row[4].split(",")), gt)
            if set(found) != set(rsids):
                raise ValueError(f"public HG00096 calls missing for {gene}")
            selected.extend(found[rsid] for rsid in rsids)
    return sorted(selected, key=lambda call: (int(call.chrom), call.pos37))


def _x_calls() -> dict[str, list[dict]]:
    _require_hash(X_SOURCE, X_SHA256)
    rows: dict[str, list[dict]] = {"HG00096": [], "HG00097": []}
    for line in X_SOURCE.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith(("#", "sample\t")):
            continue
        sample, chrom, pos, ref, alt, gt = line.split("\t")
        if sample not in rows or chrom != "X" or not re.fullmatch(r"[01](?:[|/][01])?", gt):
            raise ValueError("unexpected public X call")
        rows[sample].append({"chrom": chrom, "pos37": int(pos), "ref": ref, "alt": alt, "gt": gt})
    if (len(rows["HG00096"]), len(rows["HG00097"])) != (3, 1):
        raise ValueError("public X subset changed")
    return rows


def _pgp_no_call(path: Path, catalog_calls: list[Call]) -> dict:
    _require_hash(path, PGP_SHA256)
    source = []
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if line.startswith("rs3745274\t"):
            source.append(line.split("\t"))
    call = next(item for item in catalog_calls if item.rsid == "rs3745274")
    if len(source) != 1 or source[0][:4] != [call.rsid, call.chrom, str(call.pos37), "--"]:
        raise ValueError("public PGP no-call source changed")
    return {"rsid": call.rsid, "gene": call.gene, "chrom": call.chrom,
            "pos37": call.pos37, "pos38": call.pos38, "genotype": "--",
            "gt": None, "call_status": "no_call"}


def _vcf(calls: list[Call], x_calls: list[dict], build: str, sample: str) -> bytes:
    rows = []
    for call in calls:
        pos = call.pos38 if build == "GRCh38" else call.pos37
        rows.append((call.chrom, pos, call.rsid, call.ref, ",".join(call.source_alt), call.source_gt))
    if build == "GRCh37":
        rows.extend((item["chrom"], item["pos37"], ".", item["ref"], item["alt"], item["gt"])
                    for item in x_calls)
    rows.sort(key=lambda row: (int(row[0]) if row[0].isdigit() else 23, row[1]))
    header = (f"##fileformat=VCFv4.2\n##reference={build}\n"
              "##source=1000Genomes phase 3 public HG00096 calls\n"
              f"#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t{sample}\n")
    body = "".join(f"{chrom}\t{pos}\t{rsid}\t{ref}\t{alt}\t.\tPASS\t.\tGT\t{gt}\n"
                   for chrom, pos, rsid, ref, alt, gt in rows)
    return (header + body).encode("utf-8")


def _bgzf_block(payload: bytes) -> bytes:
    compressor = zlib.compressobj(wbits=-15)
    compressed = compressor.compress(payload) + compressor.flush()
    length = 18 + len(compressed) + 8
    if length > 65536:
        raise ValueError("public example exceeds BGZF block size")
    header = (b"\x1f\x8b\x08\x04\x00\x00\x00\x00\x00\xff\x06\x00BC\x02\x00"
              + struct.pack("<H", length - 1))
    return header + compressed + struct.pack("<II", zlib.crc32(payload), len(payload))


def _zip_vcf(vcf: bytes, calls: list[Call]) -> bytes:
    out = io.BytesIO()
    bed = "".join(f"chr{call.chrom}\t{call.pos37 - 1}\t{call.pos37}\n" for call in calls)
    with zipfile.ZipFile(out, "w") as archive:
        for name, payload in (("variants.vcf", vcf), ("regions.bed", bed.encode()),
                              ("readme.txt", b"Public 1000 Genomes HG00096 subset\n")):
            member = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            member.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(member, payload)
    return out.getvalue()


def build(regions_dir: Path, pgp_path: Path, out: Path) -> dict:
    calls = _region_calls(regions_dir)
    x_calls = _x_calls()
    no_call = _pgp_no_call(pgp_path, calls)
    out.mkdir(parents=True, exist_ok=True)
    examples: dict[str, bytes] = {}
    arrays = [(call.rsid, call.chrom, call.pos37, "".join(sorted(call.bases))) for call in calls]
    examples["hg00096-wegene.txt"] = (
        "# Generated by WeGene; public HG00096 calls, GRCh37\n"
        "rsid\tchromosome\tposition\tgenotype\n" +
        "".join(f"{rsid}\t{chrom}\t{pos}\t{bases}\n" for rsid, chrom, pos, bases in arrays)
    ).encode()
    examples["hg00096-23andme.txt"] = (
        "# 23andMe layout from public HG00096 calls; GRCh37\n"
        "# rsid\tchromosome\tposition\tgenotype\n" +
        "".join(f"{rsid}\t{chrom}\t{pos}\t{bases}\n" for rsid, chrom, pos, bases in arrays)
    ).encode()
    examples["hg00096-ancestry.txt"] = (
        "# AncestryDNA layout from public HG00096 calls; build 37\n"
        "rsid\tchromosome\tposition\tallele1\tallele2\n" +
        "".join(f"{rsid}\t{chrom}\t{pos}\t{bases[0]}\t{bases[1]}\n"
                for rsid, chrom, pos, bases in arrays)
    ).encode()
    examples["hg00096-myheritage.csv"] = (
        "# MyHeritage layout from public HG00096 calls; GRCh37\n"
        "RSID,CHROMOSOME,POSITION,RESULT\n" +
        "".join(f"{rsid},{chrom},{pos},{bases}\n" for rsid, chrom, pos, bases in arrays)
    ).encode()
    examples["hg00096-ftdna.csv"] = (
        "# FamilyTreeDNA layout from public HG00096 calls; GRCh37\n"
        "# name,chromosome,position,allele1,allele2\n" +
        "".join(f"{rsid},{chrom},{pos},{bases[0]},{bases[1]}\n"
                for rsid, chrom, pos, bases in arrays)
    ).encode()
    vcf37 = _vcf(calls, x_calls["HG00096"], "GRCh37", "HG00096")
    examples["hg00096-grch37.vcf"] = vcf37
    examples["hg00096-grch38.vcf"] = _vcf(calls, [], "GRCh38", "HG00096")
    examples["hg00096-grch37.vcf.gz"] = gzip.compress(vcf37, mtime=0)
    midpoint = len(vcf37) // 2
    examples["hg00096-grch37.vcf.bgz"] = (
        _bgzf_block(vcf37[:midpoint]) + _bgzf_block(vcf37[midpoint:]) + _bgzf_block(b"")
    )
    examples["hg00096-grch37-vcf-sidecars.zip"] = _zip_vcf(vcf37, calls)
    examples["pgp4220-nocall-23andme.txt"] = (
        "# 23andMe public PGP4220 no-call; GRCh37\n"
        "# rsid\tchromosome\tposition\tgenotype\n"
        f"{no_call['rsid']}\t{no_call['chrom']}\t{no_call['pos37']}\t--\n"
    ).encode()
    female = x_calls["HG00097"][0]
    examples["hg00097-x-grch37.vcf"] = (
        "##fileformat=VCFv4.2\n##reference=GRCh37\n"
        "##source=1000Genomes phase 3 public HG00097 X call\n"
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tHG00097\n"
        f"X\t{female['pos37']}\t.\t{female['ref']}\t{female['alt']}\t.\tPASS\t.\tGT\t{female['gt']}\n"
    ).encode()
    canonical = {
        "schema": "mirobody-public-genotype-canonical-1",
        "definition": "dbSNP rsID + assembly positions + VCF REF/ALT/GT + call status; HGNC gene where known",
        "hg00096": [
            {"rsid": call.rsid, "gene": call.gene, "chrom": call.chrom,
             "pos37": call.pos37, "pos38": call.pos38, "ref": call.ref,
             "alt": ",".join(call.catalog_alt), "array_gt": call.array_gt,
             "vcf_gt": call.vcf_gt, "call_status": "called",
             "zygosity": "homozygous" if len(set(call.bases)) == 1 else "heterozygous"}
            for call in calls
        ],
        "public_x": x_calls,
        "pgp4220_no_call": no_call,
    }
    examples["canonical.json"] = (json.dumps(canonical, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
    for name, payload in examples.items():
        (out / name).write_bytes(payload)
    manifest = {
        "schema": "mirobody-public-genotype-examples-1",
        "source": {
            "1000g_phase3_region_directory": REGION_URL,
            "region_extract_sha256": REGION_SHA256,
            "public_x_url": X_URL, "public_x_subset_sha256": X_SHA256,
            "pgp4220_url": PGP_URL, "pgp4220_raw_sha256": PGP_SHA256,
            "dbsnp_candidate_sha256": CATALOG_SHA256,
        },
        "files": [{"name": name, "bytes": len(payload),
                   "sha256": hashlib.sha256(payload).hexdigest()}
                  for name, payload in sorted(examples.items())],
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return {"files": len(examples), "bytes": sum(len(payload) for payload in examples.values()),
            "autosomal_calls": len(calls), "genes": len(REGIONS)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--regions-dir", type=Path, required=True)
    parser.add_argument("--pgp-v5", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=ROOT / "mirobody/testing/genomics")
    args = parser.parse_args()
    print(json.dumps(build(args.regions_dir, args.pgp_v5, args.out), sort_keys=True))
