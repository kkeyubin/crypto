# Crypto Research

Personal, auditable cryptocurrency research system. Phase 0 establishes contracts, the unified research Skill, API health, and the Chinese-first Web shell.

No market ingestion, backtest execution, paper order service, or signal notifications exists in Phase 0. There are no exchange credentials, real or simulated order services, or AI trading authority in this release. Planned work is documented in [docs/roadmap.md](docs/roadmap.md).

## Local checks

From the repository root in a clean checkout, use the repository's Node version and install dependencies locally:

```bash
cd services/api
python3.12 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/pytest -q
.venv/bin/ruff check src tests
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

## Server smoke run

The commands below are the reproducible server procedure; the remote smoke result is not asserted here. On `keyubin@192.168.1.4`, keep runtime secrets outside the repository and create the required directories:

```bash
sudo install -d -m 0750 /srv/crypto-research/config /srv/crypto-research/data
sudo cp /srv/crypto-research/repo/.env.example /srv/crypto-research/config/runtime.env
sudo chmod 0600 /srv/crypto-research/config/runtime.env
sudoedit /srv/crypto-research/config/runtime.env
```

Replace `POSTGRES_PASSWORD` and `CRYPTO_SESSION_SECRET` with distinct, non-example values; the session secret must contain at least 32 characters. Then run:

```bash
cd /srv/crypto-research/repo/deploy
docker compose --env-file /srv/crypto-research/config/runtime.env -f /srv/crypto-research/repo/deploy/compose.yaml config --quiet
docker compose --env-file /srv/crypto-research/config/runtime.env -f /srv/crypto-research/repo/deploy/compose.yaml up -d --build
docker compose --env-file /srv/crypto-research/config/runtime.env -f /srv/crypto-research/repo/deploy/compose.yaml ps
curl --fail http://127.0.0.1:8088/api/health/live
```

The expected live-health JSON shape is `{"status":"ok","service":"api","version":"0.1.0"}`. The Web service is deliberately bound only to `127.0.0.1:8088`; configure any LAN access only after separate approval.

To run the service through systemd after copying `deploy/crypto-research.service` to `/etc/systemd/system/crypto-research.service`:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now crypto-research.service
sudo systemctl status crypto-research.service
```

## Safety boundaries

Do not commit source books, market data, reports, generated artifacts, wallet files, exchange credentials, populated `.env` files, or other secrets. See [docs/roadmap.md](docs/roadmap.md) for the approved later phases.
