# Crypto Research

Personal, auditable cryptocurrency research system. Phase 1 adds a Binance USDⓈ-M public-data foundation: a dynamic watchlist, verified historical archives, routed live streams, independent symbol profiles, gaps, checksums, and fail-closed eligibility. The default console language is Chinese.

Phase 1 does **not** include strategies, backtests, paper orders, signal notifications, exchange credentials, or AI trading authority. Those remain separate later-phase work in [docs/roadmap.md](docs/roadmap.md).

## Local verification

Node.js is managed by NVM. From a clean checkout:

```bash
cd services/api
python3.12 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/pytest -q
.venv/bin/ruff check src tests migrations ../../deploy/api-entrypoint.py ../../deploy/market-worker-healthcheck.py
.venv/bin/python scripts/export_schemas.py --check

cd ../..
source /Users/kyle/.nvm/nvm.sh
nvm use
npm ci
npm run contracts:test-generation
npm run contracts:check-types
npm run contracts:types
git diff --exit-code contracts
npm run web:test -- --run
npm run web:build
git diff --check
```

## Fresh server installation only

Use this procedure only when there is no existing database, Compose volume, or runtime environment. An administrator needs Docker Engine with Compose, `sudo`, and a reviewed checkout. Runtime data and secrets stay outside Git:

```bash
sudo install -d -m 0755 /srv/crypto-research
sudo git clone <REPOSITORY_URL> /srv/crypto-research/repo
sudo install -d -m 0750 /srv/crypto-research/config /srv/crypto-research/data
sudo cp /srv/crypto-research/repo/.env.example /srv/crypto-research/config/runtime.env
sudo chmod 0600 /srv/crypto-research/config/runtime.env
openssl rand -hex 32  # POSTGRES_PASSWORD; copy once into runtime.env
openssl rand -hex 32  # CRYPTO_SESSION_SECRET; copy once into runtime.env
sudoedit /srv/crypto-research/config/runtime.env
```

Do not run `cp .env.example` or generate a new `POSTGRES_PASSWORD` when upgrading an existing installation. Do not paste secret values into commands or logs. The entrypoint reads `POSTGRES_PASSWORD`, constructs a percent-encoded DSN in-process, removes the raw variable from the application process, and runs migrations only in the API container.

Validate and start the server profile:

```bash
docker compose --env-file /srv/crypto-research/config/runtime.env \
  -f /srv/crypto-research/repo/deploy/compose.yaml \
  --profile server config --quiet
docker compose --env-file /srv/crypto-research/config/runtime.env \
  -f /srv/crypto-research/repo/deploy/compose.yaml \
  --profile server up -d --build
docker compose --env-file /srv/crypto-research/config/runtime.env \
  -f /srv/crypto-research/repo/deploy/compose.yaml \
  --profile server ps
curl --fail http://127.0.0.1:8088/api/health/ready
```

## Existing Phase 0 smoke upgrade

The verified smoke installation is `/home/keyubin/crypto-research-phase0-smoke`; it uses its own private `runtime.env`, bind-mounted data tree, and `crypto-research_postgres-data` volume. Upgrade it as existing state, not as a fresh installation:

1. Export the exact current paths and execute the complete `VERIFIED` backup gate in [Market Data Recovery](docs/runbooks/market-data-recovery.md).
2. Keep API/Web stopped after the backup; do not overwrite the old checkout.
3. Clone the reviewed Phase 1 commit into a separate candidate directory.
4. Reuse the existing `runtime.env` byte-for-byte, especially its `POSTGRES_PASSWORD`, and the existing data root. Never copy `.env.example` over it or generate replacement secrets for the existing database.
5. Validate the candidate Compose model before using the same `crypto-research` project/volume identity.

```bash
export CRYPTO_ENV_FILE=/home/keyubin/crypto-research-phase0-smoke/runtime.env
export CRYPTO_DATA_ROOT=/home/keyubin/crypto-research-phase0-smoke/data
export CRYPTO_BACKUP_ROOT=/home/keyubin/crypto-research-backups
export CRYPTO_CHECKOUT=/home/keyubin/crypto-research-phase1-candidate
export CRYPTO_COMPOSE_FILE="$CRYPTO_CHECKOUT/deploy/compose.yaml"
backup_root=/home/keyubin/crypto-research-backups/20260722T000000Z  # replace with the exact verified snapshot

test -f "$backup_root/VERIFIED"
test "$(stat -c %a "$CRYPTO_ENV_FILE")" = 600
test -d "$CRYPTO_DATA_ROOT"
test -f "$CRYPTO_COMPOSE_FILE"
docker compose -p crypto-research --env-file "$CRYPTO_ENV_FILE" \
  -f "$CRYPTO_COMPOSE_FILE" --profile server config --quiet
docker compose -p crypto-research --env-file "$CRYPTO_ENV_FILE" \
  -f "$CRYPTO_COMPOSE_FILE" --profile server build
docker compose -p crypto-research --env-file "$CRYPTO_ENV_FILE" \
  -f "$CRYPTO_COMPOSE_FILE" --profile server up -d
curl --fail http://127.0.0.1:8088/api/health/ready
```

The `backup_root` variable is the exact timestamped directory produced by the backup gate. If `VERIFIED` is absent, stop. Do not create it manually. Preserve the old checkout and backup until database migrations, data checksums, gaps, eligibility, restart idempotency, and loopback exposure all pass.

The server profile runs PostgreSQL, API, Web, and the separately supervised `market-worker`. PostgreSQL is reachable by the host-network worker only at `127.0.0.1:55432`; Web is reachable only at `127.0.0.1:8088`. API has no host port. The worker tries direct WebSocket access first and uses the scoped `http://127.0.0.1:17891` proxy only after a classified direct failure. Generic `HTTP_PROXY`, `HTTPS_PROXY`, and `ALL_PROXY` variables are not configured.

To supervise Compose with systemd, install `deploy/crypto-research.service`, then run `sudo systemctl daemon-reload && sudo systemctl enable --now crypto-research.service`.

From a client machine, open the console through SSH rather than exposing a LAN/public port:

```bash
ssh -N -L 8088:127.0.0.1:8088 keyubin@192.168.1.4
```

Then browse to `http://127.0.0.1:8088`. Operational onboarding, checksum verification, gap triage, backups, and rollback are in [Binance Data Operations](docs/runbooks/binance-data-operations.md) and [Market Data Recovery](docs/runbooks/market-data-recovery.md).

The isolated smoke stack was inventoried at source SHA `0226feba8bbefec207c7eee5b40c93c68a22e922`. Phase 1 server acceptance is still a completion gate; see the roadmap.

## Repository safety

Do not commit source books, market/runtime data, reports, wallet files, exchange credentials, populated `.env` files, proxy logs, or secrets. Committed contracts and reviewed SDD reports are the documented exceptions.
