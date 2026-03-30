#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/bench/reports"
RAW_OUT="${OUT_DIR}/raw.txt"
MD_OUT="${OUT_DIR}/latest.md"

COUNT="${COUNT:-5}"
BENCHTIME="${BENCHTIME:-2s}"
CPU="${CPU:-1}"

mkdir -p "${OUT_DIR}"

cd "${ROOT_DIR}"

echo "Running benchmark compare..."
echo "  COUNT=${COUNT} BENCHTIME=${BENCHTIME} CPU=${CPU}"

go test ./internal/engine \
  -run '^$' \
  -bench '^BenchmarkLSMCompare/' \
  -benchmem \
  -count "${COUNT}" \
  -benchtime "${BENCHTIME}" \
  -cpu "${CPU}" \
  > "${RAW_OUT}"

awk '
BEGIN {
  print "| Engine | Workload | ns/op(avg) | B/op(avg) | allocs/op(avg) | runs |"
  print "|---|---|---:|---:|---:|---:|"
}
/^BenchmarkLSMCompare\// {
  name=$1
  ns=$3
  b=$5
  alloc=$7

  sub(/^BenchmarkLSMCompare\//, "", name)
  sub(/-[0-9]+$/, "", name)
  split(name, parts, "/")
  engine=parts[1]
  workload=parts[2]
  sub(/^engine=/, "", engine)
  sub(/^workload=/, "", workload)

  key=engine "|" workload
  sum_ns[key]+=ns
  sum_b[key]+=b
  sum_alloc[key]+=alloc
  cnt[key]+=1
}
END {
  for (k in cnt) {
    split(k, p, "|")
    engine=p[1]
    workload=p[2]
    printf("| %s | %s | %.0f | %.0f | %.2f | %d |\n", engine, workload, sum_ns[k]/cnt[k], sum_b[k]/cnt[k], sum_alloc[k]/cnt[k], cnt[k])
  }
}
' "${RAW_OUT}" > "${MD_OUT}"

echo "Report generated:"
echo "  ${MD_OUT}"
echo
cat "${MD_OUT}"
