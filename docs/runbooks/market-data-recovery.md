# Market Data Recovery Runbook

Phase 1 recovery covers public market data, PostgreSQL catalog evidence, immutable files, gaps, audit history, and eligibility only. Later trading and notification behavior is not implemented and has no recovery action here.

## Select and Validate the Installation

For the existing smoke stack:

```bash
export CRYPTO_CHECKOUT=/home/keyubin/crypto-research-phase0-smoke
export CRYPTO_ENV_FILE=/home/keyubin/crypto-research-phase0-smoke/config/runtime.env
export CRYPTO_DATA_ROOT="$CRYPTO_CHECKOUT/data"
export CRYPTO_BACKUP_ROOT=/home/keyubin/crypto-research-backups
export CRYPTO_COMPOSE_FILE="$CRYPTO_CHECKOUT/deploy/compose.yaml"
```

For the formal installation:

```bash
export CRYPTO_CHECKOUT=/srv/crypto-research/repo
export CRYPTO_ENV_FILE=/srv/crypto-research/config/runtime.env
export CRYPTO_DATA_ROOT=/srv/crypto-research/data
export CRYPTO_BACKUP_ROOT=/srv/crypto-research/backups
export CRYPTO_COMPOSE_FILE="$CRYPTO_CHECKOUT/deploy/compose.yaml"
```

Reject an ambiguous or unsafe selection:

```bash
test -n "$CRYPTO_CHECKOUT" && test -d "$CRYPTO_CHECKOUT"
test -n "$CRYPTO_ENV_FILE" && test -f "$CRYPTO_ENV_FILE"
test -n "$CRYPTO_DATA_ROOT" && test "$CRYPTO_DATA_ROOT" != / && test -d "$CRYPTO_DATA_ROOT"
test -n "$CRYPTO_BACKUP_ROOT" && test "$CRYPTO_BACKUP_ROOT" != /
test -f "$CRYPTO_COMPOSE_FILE"
test "$(stat -c %a "$CRYPTO_ENV_FILE")" = 600
docker compose --env-file "$CRYPTO_ENV_FILE" -f "$CRYPTO_COMPOSE_FILE" config --quiet
```

## Verified Backup Gate Before Upgrade

Back up the current installation before checking out, swapping to, or starting new application code. The backup directory and environment copy remain private. Stop every current data mutator while leaving PostgreSQL available for its consistent logical dump; the Phase 0 stack may not have `market-worker`, so discover services first.

```bash
set -euo pipefail
backup_root="$CRYPTO_BACKUP_ROOT/$(date -u +%Y%m%dT%H%M%SZ)"
sudo install -d -m 0700 -o "$(id -un)" -g "$(id -gn)" \
  "$CRYPTO_BACKUP_ROOT" "$backup_root"

cd "$CRYPTO_CHECKOUT"
services=$(docker compose --env-file "$CRYPTO_ENV_FILE" -f "$CRYPTO_COMPOSE_FILE" --profile server config --services)
for service in market-worker api web; do
  if printf '%s\n' "$services" | grep -Fxq "$service"; then
    docker compose --env-file "$CRYPTO_ENV_FILE" -f "$CRYPTO_COMPOSE_FILE" --profile server stop "$service"
  fi
done

sudo install -m 0600 "$CRYPTO_ENV_FILE" "$backup_root/runtime.env"
docker compose --env-file "$CRYPTO_ENV_FILE" -f "$CRYPTO_COMPOSE_FILE" \
  --profile server exec -T postgres pg_dump -U crypto -d crypto_research -Fc \
  | sudo tee "$backup_root/catalog.dump" >/dev/null
sudo tar --acls --xattrs -cpf "$backup_root/data.tar" -C "$CRYPTO_DATA_ROOT" .

docker compose --env-file "$CRYPTO_ENV_FILE" -f "$CRYPTO_COMPOSE_FILE" \
  --profile server config --services | sudo tee "$backup_root/compose-services.txt" >/dev/null
docker compose --env-file "$CRYPTO_ENV_FILE" -f "$CRYPTO_COMPOSE_FILE" \
  --profile server config --volumes | sudo tee "$backup_root/compose-volumes.txt" >/dev/null
docker volume ls --format '{{.Name}}' | sort \
  | sudo tee "$backup_root/docker-volumes.txt" >/dev/null
sudo awk -F= '/^[A-Za-z_][A-Za-z0-9_]*=/{print $1}' "$CRYPTO_ENV_FILE" \
  | sort | sudo tee "$backup_root/runtime-env-keys.txt" >/dev/null
if git -C "$CRYPTO_CHECKOUT" rev-parse --verify HEAD >/dev/null 2>&1; then
  git -C "$CRYPTO_CHECKOUT" rev-parse HEAD \
    | sudo tee "$backup_root/checkout-identity.txt" >/dev/null
else
  sha256sum "$CRYPTO_CHECKOUT/README.md" \
    | sudo tee "$backup_root/checkout-identity.txt" >/dev/null
fi
```

Validate the private environment byte-for-byte, list the PostgreSQL dump, list the data archive, require inventories, and only then seal the backup:

```bash
set -euo pipefail
sudo cmp --silent "$CRYPTO_ENV_FILE" "$backup_root/runtime.env"
test "$(sudo stat -c %a "$backup_root/runtime.env")" = 600
docker compose --env-file "$CRYPTO_ENV_FILE" -f "$CRYPTO_COMPOSE_FILE" \
  --profile server exec -T postgres pg_restore --list < "$backup_root/catalog.dump" >/dev/null
sudo tar -tf "$backup_root/data.tar" >/dev/null
sudo test -s "$backup_root/catalog.dump"
sudo test -s "$backup_root/compose-services.txt"
sudo test -s "$backup_root/compose-volumes.txt"
sudo test -s "$backup_root/docker-volumes.txt"
sudo test -s "$backup_root/runtime-env-keys.txt"
sudo test -s "$backup_root/checkout-identity.txt"
(
  cd "$backup_root"
  sudo sha256sum catalog.dump data.tar runtime.env \
    | sudo tee "$backup_root/backup.sha256" >/dev/null
  sudo sha256sum --check "$backup_root/backup.sha256"
)
sudo touch "$backup_root/VERIFIED"
sudo test -f "$backup_root/VERIFIED"
```

**Do not start an upgrade unless `VERIFIED` exists.** If the upgrade is cancelled, restart the stopped services from the old checkout. If it proceeds, keep them stopped until the candidate checkout and Compose configuration have been validated.

For an existing database/volume, reuse the exact backed-up `runtime.env`, including its existing `POSTGRES_PASSWORD`. Never copy `.env.example` over it, generate a replacement database password, or start the same PostgreSQL volume with another password. Add only reviewed missing non-secret keys without changing existing values. Preserve the same Compose project/volume identity during the upgrade.

## Detection and Automatic Response

For each symbol and stream, track exchange time, receive time, IDs, last finalized bar, reconnect count, and age.

1. Preserve `spool/binance/usdm/bucket=00..3f/`; never delete SQLite journal, WAL, or SHM files while the writer runs.
2. Restart only the authoritative worker on the same `CRYPTO_DATA_ROOT`; it must verify prepared artifacts and resubmit published batches before catalog approval.
3. Mark affected streams degraded and make eligibility false with explicit reason codes.
4. Continue unaffected public-data ingestion only.
5. Reconnect with bounded backoff and persist every direct/proxy source-mode change.
6. Commit the disconnect or `worker_restart` gap before closing its anchor.
7. Fetch only the exact missing interval from an approved Binance archive/REST source.
8. Normalize and validate repaired rows.
9. Append manifest/checksum and audit evidence without rewriting prior records.
10. Re-evaluate eligibility only after continuity, approved-catalog, freshness, and gap checks pass.

Only PostgreSQL `live_data_partitions` rows with `layer=normalized` and `approval_status=approved` may enter DuckDB. A Parquet file on disk is not approval evidence.

## Rollback and Restore

An application-only rollback is allowed only when the previous reviewed commit supports the applied database revision. Never guess an Alembic downgrade. If catalog schemas or files are incompatible, use one matched backup:

1. Verify `VERIFIED` and `sha256sum --check "$backup_root/backup.sha256"`.
2. Stop Web, worker, and API; keep current files and database quarantined rather than deleting them.
3. Extract `data.tar` to a new path and verify its inventory before changing `CRYPTO_DATA_ROOT`.
4. Restore `catalog.dump` into an empty matched PostgreSQL database.
5. Restore the backed-up `runtime.env` at mode `0600`; its database password belongs to that database/volume.
6. Start API, then worker and Web; verify migrations, manifests, Parquet checksums, heartbeat, gaps, eligibility, and loopback listeners.

If any evidence differs, keep the worker stopped and preserve both current and backup copies.

## Legacy Spool and Migration 0004

Migration `20260721_0004` expects `live_data_partitions` to be empty because earlier source event times cannot prove canonical query ranges. With the worker stopped, inspect without following symlinks:

```bash
find -P "$CRYPTO_DATA_ROOT/spool/binance/usdm" \
  -type f -path '*/date=*/journal.sqlite3' -print
docker compose --env-file "$CRYPTO_ENV_FILE" -f "$CRYPTO_COMPOSE_FILE" \
  exec -T postgres psql -U crypto -d crypto_research -Atc \
  'SELECT count(*) FROM live_data_partitions;'
```

If legacy files or unexpected rows exist, keep the worker stopped and preserve the full tree and a data-only dump in the verified recovery area. Do not derive canonical min/max from unverified source-event fields, merge SQLite journals by filesystem copy, or delete the only evidence. Any cleanup requires a separately reviewed, audited recovery that derives ranges from checksum-verified Parquet.

Only after the query returns zero and the matched backup has `VERIFIED`, exercise the API image's normal migration entrypoint without overriding it:

```bash
cd "$CRYPTO_CHECKOUT/deploy"
docker compose --env-file "$CRYPTO_ENV_FILE" -f compose.yaml run --rm api true
```

The `true` command runs only after the image entrypoint constructs its private database URL and completes `alembic upgrade head`. Never add an entrypoint override to this check.

## Fail-Closed Conditions

Keep eligibility false and record the reason when required data is stale/missing, repaired bytes conflict with approved evidence, contract metadata changed without a new manifest, an unsafe spool layout exists, or migration `0004` encounters rows without independently verified canonical ranges. Continue only safe ingestion or investigation; do not manufacture readiness.
