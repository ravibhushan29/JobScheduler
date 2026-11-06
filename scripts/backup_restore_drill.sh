#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Backup & restore drill: prove that a backup of the job database can be
# restored and used. Run it regularly (e.g. monthly) — a backup you have never
# restored is a hope, not a backup.
#
#   scripts/backup_restore_drill.sh <compose-project>          # docker compose stack
#   PGURL=postgresql://user:pw@host:5432/jobs scripts/backup_restore_drill.sh --url   # any Postgres
#
# Steps:
#   1. pg_dump the live database (custom format, consistent snapshot; no downtime).
#   2. Start a throwaway Postgres and pg_restore the dump into it.
#   3. Compare row counts of every table, live vs restored (the live side may
#      have grown while the dump ran, so restored <= live is expected).
#   4. Run the app's migrations against the restored copy (python -m app.migrate)
#      and read the job history through the app's own code.
#   5. Report the dump size and how long each step took (your RTO estimate).
# The throwaway database is always removed; the dump file is kept.
# ---------------------------------------------------------------------------
set -euo pipefail

MODE="${1:?usage: $0 <compose-project> | --url}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${OUT_DIR:-./backups}"
DUMP="$OUT_DIR/jobs-$STAMP.dump"
SCRATCH="drill-pg-$STAMP"
PG_IMAGE="${PG_IMAGE:-postgres:16-alpine}"
mkdir -p "$OUT_DIR"

cleanup() { docker rm -f "$SCRATCH" >/dev/null 2>&1 || true; }
trap cleanup EXIT

elapsed() { echo $(( $(date +%s) - $1 )); }

# --- 1. dump ---------------------------------------------------------------
t0=$(date +%s)
if [[ "$MODE" == "--url" ]]; then
  : "${PGURL:?set PGURL}"
  docker run --rm --network host -v "$PWD/$OUT_DIR:/out" "$PG_IMAGE" \
    pg_dump --format=custom --no-owner --file="/out/$(basename "$DUMP")" "$PGURL"
  live_psql() { docker run --rm --network host "$PG_IMAGE" psql "$PGURL" -At -c "$1"; }
else
  PROJECT="$MODE"
  PG_CONTAINER="$PROJECT-postgres-1"
  docker exec "$PG_CONTAINER" pg_dump -U jobs -d jobs --format=custom --no-owner >"$DUMP"
  live_psql() { docker exec "$PG_CONTAINER" psql -U jobs -d jobs -At -c "$1"; }
fi
dump_seconds=$(elapsed "$t0")
echo "1. dump:     $DUMP ($(du -h "$DUMP" | cut -f1)) in ${dump_seconds}s"

# --- 2. restore into a throwaway Postgres ----------------------------------
t1=$(date +%s)
docker run -d --name "$SCRATCH" -e POSTGRES_PASSWORD=drill -e POSTGRES_DB=jobs -p 127.0.0.1::5432 "$PG_IMAGE" >/dev/null
until docker exec "$SCRATCH" pg_isready -U postgres -d jobs -h 127.0.0.1 >/dev/null 2>&1; do sleep 1; done
docker exec -i "$SCRATCH" pg_restore -U postgres -d jobs --no-owner --exit-on-error <"$DUMP"
restore_seconds=$(elapsed "$t1")
echo "2. restore:  into $SCRATCH in ${restore_seconds}s"

# --- 3. compare row counts --------------------------------------------------
scratch_psql() { docker exec "$SCRATCH" psql -U postgres -d jobs -At -c "$1"; }
tables=$(scratch_psql "select tablename from pg_tables where schemaname='public' order by 1")
problems=0
printf '3. compare:  %-22s %10s %10s\n' table live restored
for table in $tables; do
  restored=$(scratch_psql "select count(*) from \"$table\"")
  live=$(live_psql "select count(*) from \"$table\"")
  flag=""
  if (( restored > live )); then flag="  <-- MORE rows than live?"; problems=$((problems + 1)); fi
  if (( restored == 0 && live > 0 )); then flag="  <-- EMPTY after restore"; problems=$((problems + 1)); fi
  printf '            %-22s %10s %10s%s\n' "$table" "$live" "$restored" "$flag"
done

# --- 4. the app can use the restored copy -----------------------------------
port=$(docker port "$SCRATCH" 5432/tcp | head -1 | awk -F: '{print $NF}')
url="postgresql://postgres:drill@127.0.0.1:$port/jobs"
if [[ -x venv/bin/python ]]; then PY=venv/bin/python; else PY=python3; fi
DATABASE_URL="$url" CELERY_BROKER_URL=memory:// LOG_TO_FILE=false LOG_LEVEL=WARNING "$PY" -m app.migrate >/dev/null
DATABASE_URL="$url" CELERY_BROKER_URL=memory:// LOG_TO_FILE=false LOG_LEVEL=WARNING "$PY" - <<'EOF'
from app import db
jobs = db.run_sync(db.list_jobs(limit=1))
total = db.run_sync(db.count_jobs())
print(f"4. app:      migrations OK; app reads {total} jobs (newest: {jobs[0]['task_id'] if jobs else '-'})")
EOF

total_seconds=$(elapsed "$t0")
echo "5. result:   restore drill took ${total_seconds}s end to end (dump ${dump_seconds}s + restore ${restore_seconds}s)"
if (( problems > 0 )); then
  echo "FAILED: $problems table(s) look wrong" >&2
  exit 1
fi
echo "PASSED"
