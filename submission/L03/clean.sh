#!/usr/bin/env bash
# keep only result tables, row counts/timings and errors from a run_sql.sh log
grep -E '^[+|]|rows? (selected|affected)|^Error|^FAILED' "$1"
