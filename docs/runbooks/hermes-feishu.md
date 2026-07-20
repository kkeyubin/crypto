# Hermes Feishu Runbook

## Service

Hermes profile: `crypto`

User unit: `hermes-gateway-crypto.service`

Check service durability:

```bash
systemctl --user is-enabled hermes-gateway-crypto.service
systemctl --user is-active hermes-gateway-crypto.service
loginctl show-user keyubin -p Linger
```

Expected results are `enabled`, `active`, and `Linger=yes`.

## Safe Test

Do not place message text in an interpolated shell command. Pipe it through stdin:

```bash
printf '%s\n' '[Crypto Research System] Feishu notification test.' \
  | ~/.local/bin/hermes -p crypto send \
      --to feishu \
      --file - \
      --json
```

Hermes exit codes are `0` success, `1` delivery/backend failure, and `2` usage error. Application delivery always goes through the PostgreSQL Outbox and records sanitized metadata only.

## Common Failure: 230002

```text
[230002] Bot/User can NOT be out of the chat.
```

The configured home channel does not contain the bot, or its saved chat is stale. Add the `crypto` bot to the intended Feishu chat, mention it once so the gateway discovers the channel, and rerun the safe test. Do not solve this by embedding a new chat ID in source code.

## Logs and Restart

```bash
journalctl --user -u hermes-gateway-crypto.service -n 100 --no-pager
systemctl --user restart hermes-gateway-crypto.service
```

After restart, verify service state and send one explicitly marked integration test. Never print Hermes `.env`, Feishu credentials, complete chat IDs, or message IDs into repository documentation or ordinary application logs.
