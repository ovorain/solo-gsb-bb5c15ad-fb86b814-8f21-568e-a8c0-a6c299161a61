"""Cross-stream atomic transfers.

A transfer request carries a client-assigned ``transfer_id`` (the idempotency
key), two *different* stream ids and a positive integer ``amount``. It is only
allowed when both streams already exist and the source stream's current balance
covers the amount. On success, inside one ``BEGIN IMMEDIATE`` transaction:

1. a debit event (``delta = -amount``) is appended to the source stream at its
   current ``next_sequence`` and a credit event (``delta = +amount``) to the
   target stream at its current ``next_sequence``;
2. both streams run the contiguous catch-up (the same merge algorithm as
   :mod:`repo`), so the appended events — and any buffered events they
   un-block — are applied atomically;
3. a ``transfers`` row records the request and the resulting snapshot.

Any failure rolls the whole transaction back: no events, no balance changes,
no transfer record are left behind. A retry with the same ``transfer_id`` and
identical parameters returns the stored original result without moving funds
again; the same ``transfer_id`` with different parameters is a ``409``.
Concurrent retries serialise on the SQLite write lock, so exactly one of them
commits the transfer.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from . import db, repo


class TransferError(Exception):
    """Raised when a transfer request is invalid or clashes (HTTP 4xx)."""

    def __init__(self, code: str, message: str, status_code: int = 409):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


@dataclass(frozen=True)
class Transfer:
    transfer_id: str
    source_stream_id: str
    target_stream_id: str
    amount: int
    source_sequence: int  # sequence of the debit event in the source stream
    target_sequence: int  # sequence of the credit event in the target stream
    source: repo.StreamState  # source state right after the transfer
    target: repo.StreamState  # target state right after the transfer
    replayed: bool

    def as_dict(self) -> dict:
        return {
            "transfer_id": self.transfer_id,
            "source_stream_id": self.source_stream_id,
            "target_stream_id": self.target_stream_id,
            "amount": self.amount,
            "source_sequence": self.source_sequence,
            "target_sequence": self.target_sequence,
            "source": self.source.as_dict(),
            "target": self.target.as_dict(),
        }


def _debit_event_id(transfer_id: str) -> str:
    return f"transfer:{transfer_id}:debit"


def _credit_event_id(transfer_id: str) -> str:
    return f"transfer:{transfer_id}:credit"


def _replay_of(row: sqlite3.Row) -> Transfer:
    """Rebuild the original result snapshot from a stored transfers row."""
    return Transfer(
        transfer_id=row["transfer_id"],
        source_stream_id=row["source_stream_id"],
        target_stream_id=row["target_stream_id"],
        amount=row["amount"],
        source_sequence=row["source_sequence"],
        target_sequence=row["target_sequence"],
        source=repo.StreamState(
            stream_id=row["source_stream_id"],
            balance=row["source_balance"],
            next_sequence=row["source_next_sequence"],
            pending_sequences=json.loads(row["source_pending"]),
        ),
        target=repo.StreamState(
            stream_id=row["target_stream_id"],
            balance=row["target_balance"],
            next_sequence=row["target_next_sequence"],
            pending_sequences=json.loads(row["target_pending"]),
        ),
        replayed=True,
    )


def execute_transfer(
    transfer_id: str,
    source_stream_id: str,
    target_stream_id: str,
    amount: int,
) -> Transfer:
    """Move ``amount`` from the source stream to the target stream atomically.

    Raises :class:`TransferError` when either stream is missing (404), the
    source balance is insufficient (409) or the ``transfer_id`` was already
    used with different parameters (409).
    """
    with db.write_transaction() as conn:
        existing = conn.execute(
            "SELECT * FROM transfers WHERE transfer_id = ?",
            (transfer_id,),
        ).fetchone()
        if existing is not None:
            # Idempotent replay: same transfer_id must carry identical parameters.
            if (
                existing["source_stream_id"] != source_stream_id
                or existing["target_stream_id"] != target_stream_id
                or existing["amount"] != amount
            ):
                raise TransferError(
                    "transfer_replay_conflict",
                    f"transfer_id {transfer_id!r} already exists with different "
                    "parameters",
                )
            return _replay_of(existing)

        source = repo._get_stream(conn, source_stream_id)
        if source is None:
            raise TransferError(
                "stream_not_found",
                f"unknown stream {source_stream_id!r}",
                status_code=404,
            )
        target = repo._get_stream(conn, target_stream_id)
        if target is None:
            raise TransferError(
                "stream_not_found",
                f"unknown stream {target_stream_id!r}",
                status_code=404,
            )

        if source["balance"] < amount:
            raise TransferError(
                "insufficient_funds",
                f"source stream {source_stream_id!r} balance "
                f"{source['balance']} is below the transfer amount {amount}",
            )

        # Append the debit/credit events at each side's current next_sequence,
        # then run the same contiguous catch-up as a regular event submission:
        # the appended events apply immediately and un-block any buffered
        # events that now form a contiguous chain.
        source_sequence = source["next_sequence"]
        conn.execute(
            "INSERT INTO events (stream_id, event_id, sequence, delta, applied) "
            "VALUES (?, ?, ?, ?, 0)",
            (source_stream_id, _debit_event_id(transfer_id), source_sequence, -amount),
        )
        target_sequence = target["next_sequence"]
        conn.execute(
            "INSERT INTO events (stream_id, event_id, sequence, delta, applied) "
            "VALUES (?, ?, ?, ?, 0)",
            (target_stream_id, _credit_event_id(transfer_id), target_sequence, amount),
        )

        source_balance, source_next = repo._catch_up(
            conn, source_stream_id, source["balance"], source_sequence
        )
        target_balance, target_next = repo._catch_up(
            conn, target_stream_id, target["balance"], target_sequence
        )

        source_state = repo.StreamState(
            stream_id=source_stream_id,
            balance=source_balance,
            next_sequence=source_next,
            pending_sequences=repo._pending_sequences(conn, source_stream_id),
        )
        target_state = repo.StreamState(
            stream_id=target_stream_id,
            balance=target_balance,
            next_sequence=target_next,
            pending_sequences=repo._pending_sequences(conn, target_stream_id),
        )

        # Record the transfer together with the result snapshot; the PRIMARY
        # KEY on transfer_id is the backstop that makes concurrent retries
        # commit at most once.
        conn.execute(
            "INSERT INTO transfers (transfer_id, source_stream_id, "
            "target_stream_id, amount, source_sequence, target_sequence, "
            "source_balance, source_next_sequence, target_balance, "
            "target_next_sequence, source_pending, target_pending) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                transfer_id,
                source_stream_id,
                target_stream_id,
                amount,
                source_sequence,
                target_sequence,
                source_state.balance,
                source_state.next_sequence,
                target_state.balance,
                target_state.next_sequence,
                json.dumps(source_state.pending_sequences),
                json.dumps(target_state.pending_sequences),
            ),
        )

        return Transfer(
            transfer_id=transfer_id,
            source_stream_id=source_stream_id,
            target_stream_id=target_stream_id,
            amount=amount,
            source_sequence=source_sequence,
            target_sequence=target_sequence,
            source=source_state,
            target=target_state,
            replayed=False,
        )
