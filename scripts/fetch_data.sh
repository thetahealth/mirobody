#!/usr/bin/env bash
#
# Fetch the terminology data that is not in the git checkout.
#
#     scripts/fetch_data.sh          # what a deployment needs (the `runtime` rows)
#     scripts/fetch_data.sh --all    # those, plus the bundle-build inputs
#
# As of 1.5.0 every row is `archive`: the bundle is cut from a LOINC release
# by `standardize_build/` and nothing in the package opens these files. Both
# commands therefore download nothing, and both still exit 0.
#     scripts/fetch_data.sh --check  # report what is present, download nothing
#
# What and why is `mirobody/res/EXTERNAL.tsv`, which this script reads rather
# than restating: one table, so the checksums a download is verified against
# and the checksums a reader sees are the same characters.
#
# Called by `./deploy.sh` and by CI. Safe to re-run: a file that is present
# and whose checksum matches is left alone, so this costs one `stat` and one
# hash on every boot after the first.
#
# It does NOT fail the caller when a download fails. The one `runtime` file
# degrades to a warning in `collect/query.py` (semantic search falls back to the
# lexical index), and a deployment that cannot reach github.com must still
# come up — that is the whole point of the offline resolver.

set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
res_dir="${here}/../mirobody/res"
manifest="${res_dir}/EXTERNAL.tsv"

want_all=0
check_only=0
for arg in "$@"; do
    case "$arg" in
        --all)   want_all=1 ;;
        --check) check_only=1 ;;
        -h|--help) sed -n '3,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "fetch_data: unknown argument '$arg'" >&2; exit 2 ;;
    esac
done

if [[ ! -f "$manifest" ]]; then
    echo "fetch_data: no manifest at ${manifest}" >&2
    exit 1
fi

release="$(awk -F'\t' '$1=="RELEASE"{print $2; exit}' "$manifest")"
if [[ -z "$release" ]]; then
    echo "fetch_data: ${manifest} names no RELEASE" >&2
    exit 1
fi
base_url="https://github.com/thetahealth/mirobody/releases/download/${release}"

sha256_of() {
    if command -v shasum &>/dev/null; then
        shasum -a 256 "$1" | cut -d' ' -f1
    elif command -v sha256sum &>/dev/null; then
        sha256sum "$1" | cut -d' ' -f1
    else
        echo ""   # no hasher: treat presence as good enough rather than refusing to boot
    fi
}

download() {
    # $1 url, $2 destination. Writes to a temp file so an interrupted
    # download never leaves a truncated artifact that passes an exists() check
    # and fails a checksum on every boot after.
    local url="$1" dest="$2" tmp="$2.partial"
    rm -f "$tmp"
    # A progress bar is for a person watching ./deploy.sh; in CI and in a
    # container log it is thousands of carriage returns.
    local on_tty=0
    [[ -t 2 ]] && on_tty=1
    if command -v curl &>/dev/null; then
        local progress="--silent"
        [[ "$on_tty" -eq 1 ]] && progress="--progress-bar"
        curl -fL --retry 3 --connect-timeout 10 "$progress" -o "$tmp" "$url" || return 1
    elif command -v wget &>/dev/null; then
        # An if/else, not `[[ -t 2 ]] && a || b`: with that shape a FAILED
        # download on a tty falls through to the `||` and retries silently,
        # so a real failure looks like a quiet success.
        if [[ "$on_tty" -eq 1 ]]; then
            wget -q --show-progress --tries=3 -O "$tmp" "$url" || return 1
        else
            wget -q --tries=3 -O "$tmp" "$url" || return 1
        fi
    else
        echo "fetch_data: neither curl nor wget is installed" >&2
        return 1
    fi
    mv "$tmp" "$dest"
}

missing=0
fetched=0

while IFS=$'\t' read -r name sha bytes need _rest; do
    [[ -z "${name:-}" || "$name" == \#* || "$name" == "RELEASE" ]] && continue
    [[ "$need" == "build" && "$want_all" -eq 0 ]] && continue
    # `archive` is a row nothing reads any more, kept so an older tag still
    # resolves its checksums. Never downloaded, by --all either.
    [[ "$need" == "archive" ]] && continue

    dest="${res_dir}/${name}"
    if [[ -f "$dest" ]]; then
        have="$(sha256_of "$dest")"
        if [[ -z "$have" || "$have" == "$sha" ]]; then
            [[ "$check_only" -eq 1 ]] && echo "  present  ${name}"
            continue
        fi
        echo "  stale    ${name} (checksum differs from EXTERNAL.tsv; re-downloading)"
    elif [[ "$check_only" -eq 1 ]]; then
        echo "  MISSING  ${name}  (${bytes} bytes, ${need})"
        missing=$((missing + 1))
        continue
    fi

    echo "  fetching ${name} (${bytes} bytes) from ${release}"
    if ! download "${base_url}/${name}" "$dest"; then
        echo "  FAILED   ${name} — ${base_url}/${name}" >&2
        missing=$((missing + 1))
        continue
    fi
    have="$(sha256_of "$dest")"
    if [[ -n "$have" && "$have" != "$sha" ]]; then
        # A wrong file is worse than no file: the resolver would answer from
        # it. Removed, so the next run tries again and the warning is honest.
        echo "  CORRUPT  ${name}: sha256 ${have} != ${sha}; removed" >&2
        rm -f "$dest"
        missing=$((missing + 1))
        continue
    fi
    fetched=$((fetched + 1))
done < "$manifest"

if [[ "$check_only" -eq 1 ]]; then
    [[ "$missing" -eq 0 ]] && echo "fetch_data: everything the manifest lists is present."
    exit 0
fi

if [[ "$missing" -gt 0 ]]; then
    echo "fetch_data: ${missing} file(s) could not be fetched. The server still starts;" >&2
    echo "            semantic indicator search falls back to the lexical index." >&2
elif [[ "$fetched" -gt 0 ]]; then
    echo "fetch_data: ${fetched} file(s) fetched into mirobody/res/."
fi
exit 0
