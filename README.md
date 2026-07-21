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

## Server deployment prerequisites

The commands below are the formal installation procedure. A server administrator needs Docker Engine with the Compose plugin, `sudo`, a reviewed repository checkout, and two new random secrets. First clone or copy the reviewed repository to `/srv/crypto-research/repo`; do this before creating the runtime env file:

```bash
sudo install -d -m 0755 /srv/crypto-research
sudo git clone <REPOSITORY_URL> /srv/crypto-research/repo
# Alternatively, copy an already-reviewed checkout to /srv/crypto-research/repo.
sudo install -d -m 0750 /srv/crypto-research/config /srv/crypto-research/data
sudo cp /srv/crypto-research/repo/.env.example /srv/crypto-research/config/runtime.env
sudo chmod 0600 /srv/crypto-research/config/runtime.env
sudoedit /srv/crypto-research/config/runtime.env
```

Generate distinct values before editing `runtime.env`:

```bash
openssl rand -hex 32  # POSTGRES_PASSWORD
openssl rand -hex 32  # CRYPTO_SESSION_SECRET
```

Use each output once. Hex is recommended for easy operator handling; the API entrypoint safely percent-encodes any PostgreSQL password before constructing its DSN. Keep both values out of shell history, logs, and the repository.

## Server smoke run

After the prerequisites are complete, an administrator may run:

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

## Verified smoke record

On 2026-07-21, the reviewed Phase 0 runtime source at `0226feba8bbefec207c7eee5b40c93c68a22e922` was smoke-tested on `keyubin@192.168.1.4` in the isolated user-owned path `/home/keyubin/crypto-research-phase0-smoke`. PostgreSQL, API, and Web were healthy with `unless-stopped`; live/readiness endpoints and the Web root passed; only `127.0.0.1:8088` was bound. The raw database password was absent from the API process after DSN construction.

Docker's first API build could not resolve PyPI. In accordance with the repository rule, the existing loopback proxy on port `17891` was used only for the restricted image-build step; it is not part of Compose, the image runtime environment, or the service configuration. The isolated smoke stack remains running. The formal `/srv/crypto-research` checkout and systemd unit were not installed because that requires administrator privileges.

## Safety boundaries

Do not commit source books, runtime data, reports, runtime outputs, wallet files, exchange credentials, populated `.env` files, or other secrets. Committed contracts and reviewed SDD reports are exceptions; they are repository records, not runtime outputs. See [docs/roadmap.md](docs/roadmap.md) for the approved later phases.
