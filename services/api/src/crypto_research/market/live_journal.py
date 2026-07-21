from __future__ import annotations

import os
import sqlite3
import stat
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path


class LiveJournalError(ValueError):
    """A durable live event conflicts with the partition journal."""


@dataclass(frozen=True)
class JournalAcceptResult:
    accepted: bool
    batch_id: str | None = None


@dataclass(frozen=True)
class JournalEvent:
    identity: str
    payload_hash: str
    event_json: str
    source_event_time: int
    normalize: bool
    partition_key: str


@dataclass(frozen=True)
class JournalBatch:
    batch_id: str
    state: str
    manifest_json: str


class LivePartitionJournal:
    """One WAL-backed fixed-bucket spool containing isolated source partitions."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._initialize()

    def accept(
        self,
        *,
        identity: str,
        payload_hash: str,
        event_json: str,
        source_event_time: int,
        partition_key: str,
        normalized_key: str | None,
        normalized_hash: str | None,
    ) -> JournalAcceptResult:
        with self._transaction() as connection:
            previous = connection.execute(
                "SELECT payload_hash, batch_id FROM events WHERE identity = ?",
                (identity,),
            ).fetchone()
            if previous is not None:
                if previous["payload_hash"] != payload_hash:
                    raise LiveJournalError(
                        "live replay conflicts with published source identity"
                    )
                return JournalAcceptResult(False, previous["batch_id"])

            normalize = False
            if normalized_key is not None:
                assert normalized_hash is not None
                normalized = connection.execute(
                    "SELECT payload_hash FROM normalized_records WHERE normalized_key = ?",
                    (normalized_key,),
                ).fetchone()
                if normalized is None:
                    connection.execute(
                        """
                        INSERT INTO normalized_records(
                            normalized_key, payload_hash, event_identity
                        ) VALUES (?, ?, ?)
                        """,
                        (normalized_key, normalized_hash, identity),
                    )
                    normalize = True
                elif normalized["payload_hash"] != normalized_hash:
                    raise LiveJournalError(
                        "live normalized primary key conflicts with existing payload"
                    )

            connection.execute(
                """
                INSERT INTO events(
                    identity, payload_hash, event_json, source_event_time,
                    normalize, partition_key, state
                ) VALUES (?, ?, ?, ?, ?, ?, 'queued')
                """,
                (
                    identity,
                    payload_hash,
                    event_json,
                    source_event_time,
                    int(normalize),
                    partition_key,
                ),
            )
        return JournalAcceptResult(True)

    def next_open_batch(self) -> JournalBatch | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT batch_id, state, manifest_json
                FROM batches
                WHERE state IN ('prepared', 'published')
                ORDER BY created_at, batch_id
                LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None
        return JournalBatch(row["batch_id"], row["state"], row["manifest_json"])

    def queued(self, limit: int) -> tuple[JournalEvent, ...]:
        with self._connect() as connection:
            partition = connection.execute(
                """
                SELECT partition_key
                FROM events
                WHERE state = 'queued'
                ORDER BY source_event_time, identity
                LIMIT 1
                """
            ).fetchone()
            if partition is None:
                return ()
            rows = connection.execute(
                """
                SELECT identity, payload_hash, event_json, source_event_time,
                       normalize, partition_key
                FROM events
                WHERE state = 'queued' AND partition_key = ?
                ORDER BY source_event_time, identity
                LIMIT ?
                """,
                (partition["partition_key"], limit),
            ).fetchall()
        return tuple(
            JournalEvent(
                row["identity"],
                row["payload_hash"],
                row["event_json"],
                row["source_event_time"],
                bool(row["normalize"]),
                row["partition_key"],
            )
            for row in rows
        )

    def prepare(
        self, batch_id: str, events: Sequence[JournalEvent], manifest_json: str
    ) -> JournalBatch:
        identities = tuple(event.identity for event in events)
        with self._transaction() as connection:
            connection.execute(
                """
                INSERT INTO batches(batch_id, state, manifest_json)
                VALUES (?, 'prepared', ?)
                """,
                (batch_id, manifest_json),
            )
            for position, identity in enumerate(identities):
                changed = connection.execute(
                    """
                    UPDATE events
                    SET state = 'prepared', batch_id = ?
                    WHERE identity = ? AND state = 'queued'
                    """,
                    (batch_id, identity),
                ).rowcount
                if changed != 1:
                    raise LiveJournalError("queued live event changed during prepare")
                connection.execute(
                    """
                    INSERT INTO batch_events(batch_id, event_identity, position)
                    VALUES (?, ?, ?)
                    """,
                    (batch_id, identity, position),
                )
        return JournalBatch(batch_id, "prepared", manifest_json)

    def batch_events(self, batch_id: str) -> tuple[JournalEvent, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT e.identity, e.payload_hash, e.event_json,
                       e.source_event_time, e.normalize, e.partition_key
                FROM batch_events AS be
                JOIN events AS e ON e.identity = be.event_identity
                WHERE be.batch_id = ?
                ORDER BY be.position
                """,
                (batch_id,),
            ).fetchall()
        if not rows or any(row["event_json"] is None for row in rows):
            raise LiveJournalError("prepared live batch has no recoverable event payload")
        return tuple(
            JournalEvent(
                row["identity"],
                row["payload_hash"],
                row["event_json"],
                row["source_event_time"],
                bool(row["normalize"]),
                row["partition_key"],
            )
            for row in rows
        )

    def mark_published(self, batch_id: str) -> None:
        with self._transaction() as connection:
            changed = connection.execute(
                """
                UPDATE batches SET state = 'published'
                WHERE batch_id = ? AND state IN ('prepared', 'published')
                """,
                (batch_id,),
            ).rowcount
            if changed != 1:
                raise LiveJournalError("live batch cannot transition to published")
            connection.execute(
                """
                UPDATE events SET state = 'published'
                WHERE batch_id = ? AND state IN ('prepared', 'published')
                """,
                (batch_id,),
            )

    def acknowledge_cataloged(self, batch_id: str) -> bool:
        with self._transaction() as connection:
            changed = connection.execute(
                """
                UPDATE batches SET state = 'cataloged'
                WHERE batch_id = ? AND state = 'published'
                """,
                (batch_id,),
            ).rowcount
            if changed == 0:
                existing = connection.execute(
                    "SELECT state FROM batches WHERE batch_id = ?", (batch_id,)
                ).fetchone()
                if existing is None:
                    return False
                if existing["state"] != "cataloged":
                    raise LiveJournalError("live batch is not published")
                return True
            connection.execute(
                """
                UPDATE events SET state = 'cataloged', event_json = NULL
                WHERE batch_id = ? AND state = 'published'
                """,
                (batch_id,),
            )
            connection.execute(
                "DELETE FROM batch_events WHERE batch_id = ?", (batch_id,)
            )
        return True

    def _initialize(self) -> None:
        try:
            journal_stat = os.lstat(self.path)
            created = False
            if (
                not stat.S_ISREG(journal_stat.st_mode)
                or stat.S_ISLNK(journal_stat.st_mode)
                or stat.S_IMODE(journal_stat.st_mode) & 0o077
            ):
                raise LiveJournalError(
                    "live journal must be a private regular file"
                )
        except FileNotFoundError:
            created = True
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS events (
                    identity TEXT PRIMARY KEY,
                    payload_hash TEXT NOT NULL,
                    event_json TEXT,
                    source_event_time INTEGER NOT NULL,
                    normalize INTEGER NOT NULL CHECK (normalize IN (0, 1)),
                    partition_key TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (
                        state IN ('queued', 'prepared', 'published', 'cataloged')
                    ),
                    batch_id TEXT
                ) WITHOUT ROWID;

                CREATE INDEX IF NOT EXISTS ix_events_state_order
                    ON events(state, partition_key, source_event_time, identity);

                CREATE TABLE IF NOT EXISTS normalized_records (
                    normalized_key TEXT PRIMARY KEY,
                    payload_hash TEXT NOT NULL,
                    event_identity TEXT NOT NULL UNIQUE
                ) WITHOUT ROWID;

                CREATE TABLE IF NOT EXISTS batches (
                    batch_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL CHECK (
                        state IN ('prepared', 'published', 'cataloged')
                    ),
                    manifest_json TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                ) WITHOUT ROWID;

                CREATE INDEX IF NOT EXISTS ix_batches_state_created
                    ON batches(state, created_at, batch_id);

                CREATE TABLE IF NOT EXISTS batch_events (
                    batch_id TEXT NOT NULL,
                    event_identity TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    PRIMARY KEY (batch_id, event_identity),
                    UNIQUE (batch_id, position),
                    FOREIGN KEY (batch_id) REFERENCES batches(batch_id),
                    FOREIGN KEY (event_identity) REFERENCES events(identity)
                ) WITHOUT ROWID;
                """
            )
        if created:
            os.chmod(self.path, 0o600)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=5)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=5000")
            yield connection
        finally:
            connection.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except BaseException:
                connection.rollback()
                raise
            else:
                connection.commit()
