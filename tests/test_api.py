"""End-to-end API tests (requires fastapi + httpx, installed via requirements-dev.txt)."""

from __future__ import annotations

import os
import tempfile

import pytest


@pytest.fixture(scope="session")
def client(tmp_path_factory):
    db_dir = tmp_path_factory.mktemp("db")
    os.environ["DATABASE_PATH"] = str(db_dir / "test_events.db")

    from fastapi.testclient import TestClient  # noqa: E402

    from app.main import app  # noqa: E402

    with TestClient(app) as c:
        yield c


def _post(client, stream_id, event_id, sequence, delta):
    return client.post(
        f"/streams/{stream_id}/events",
        json={"event_id": event_id, "sequence": sequence, "delta": delta},
    )


def test_in_order_events_accumulate(client):
    r = _post(client, "s-ordered", "e0", 0, 100)
    assert r.status_code == 201
    assert r.json()["stream"] == {
        "stream_id": "s-ordered",
        "balance": 100,
        "next_sequence": 1,
        "pending_sequences": [],
    }

    r = _post(client, "s-ordered", "e1", 1, -30)
    assert r.status_code == 201
    assert r.json()["stream"]["balance"] == 70
    assert r.json()["stream"]["next_sequence"] == 2


def test_gap_is_buffered_then_caught_up_atomically(client):
    # Sequence 1 arrives before sequence 0: nothing is applied yet.
    r = _post(client, "s-gap", "e1", 1, 200)
    assert r.status_code == 201
    body = r.json()
    assert body["stream"]["balance"] == 0
    assert body["stream"]["next_sequence"] == 0
    assert body["stream"]["pending_sequences"] == [1]

    r = client.get("/streams/s-gap")
    assert r.json()["pending_sequences"] == [1]

    # Missing event arrives: both events are applied in the same transaction.
    r = _post(client, "s-gap", "e0", 0, 50)
    assert r.status_code == 201
    body = r.json()
    assert body["stream"]["balance"] == 250
    assert body["stream"]["next_sequence"] == 2
    assert body["stream"]["pending_sequences"] == []


def test_replay_is_idempotent(client):
    _post(client, "s-rep", "e0", 0, 10)

    r = _post(client, "s-rep", "e0", 0, 10)
    assert r.status_code == 200
    assert r.json()["replayed"] is True
    assert r.json()["stream"]["balance"] == 10
    assert r.json()["stream"]["next_sequence"] == 1


def test_replay_with_different_content_returns_409(client):
    _post(client, "s-conf", "e0", 0, 10)

    r = _post(client, "s-conf", "e0", 1, 10)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "replay_conflict"

    r = _post(client, "s-conf", "e0", 0, 999)
    assert r.status_code == 409


def test_duplicate_sequence_different_event_id_returns_409(client):
    _post(client, "s-dup", "e0", 0, 10)

    r = _post(client, "s-dup", "other-event", 0, 10)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "sequence_conflict"
    # The conflicting event was never stored.
    assert client.get("/streams/s-dup").json()["next_sequence"] == 1


def test_get_unknown_stream_returns_404(client):
    r = client.get("/streams/does-not-exist")
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "stream_not_found"


def test_validation_rejects_bad_payloads(client):
    assert _post(client, "s-val", "", 0, 1).status_code == 422
    assert _post(client, "s-val", "e", -1, 1).status_code == 422
    assert _post(client, "s-val", "e", 0, "not-an-int").status_code == 422
    assert _post(client, "s-val", "e", True, 1).status_code == 422


def test_large_out_of_order_batch_catches_up(client):
    # Submit 1..9 (all buffered because 0 is missing), then 0.
    for seq in range(1, 10):
        r = _post(client, "s-batch", f"e{seq}", seq, seq)
        assert r.json()["stream"]["next_sequence"] == 0
    assert client.get("/streams/s-batch").json()["pending_sequences"] == list(range(1, 10))

    r = _post(client, "s-batch", "e0", 0, 0)
    body = r.json()
    assert body["stream"]["next_sequence"] == 10
    assert body["stream"]["balance"] == sum(range(10))
    assert body["stream"]["pending_sequences"] == []


def test_concurrent_submissions_apply_each_sequence_once(client):
    import threading

    def post(seq):
        _post(client, "s-conc", f"e{seq}", seq, 1)

    threads = [threading.Thread(target=post, args=(seq,)) for seq in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    state = client.get("/streams/s-conc").json()
    assert state["next_sequence"] == 20
    assert state["balance"] == 20
    assert state["pending_sequences"] == []


# --------------------------------------------------------------------------- #
# Consumer cursor: claim / ack leases
# --------------------------------------------------------------------------- #


def _seed_applied_events(client, stream_id, count, start=0):
    for seq in range(start, start + count):
        r = _post(client, stream_id, f"{stream_id}-e{seq}", seq, seq + 1)
        assert r.status_code in (200, 201)


def _claim(client, stream_id, consumer_id, max_events):
    return client.post(
        f"/streams/{stream_id}/consumers/{consumer_id}/claim",
        json={"max_events": max_events},
    )


def _ack(client, stream_id, consumer_id, token):
    return client.post(
        f"/streams/{stream_id}/consumers/{consumer_id}/ack",
        json={"lease_token": token},
    )


def test_claim_returns_contiguous_applied_batch(client):
    _seed_applied_events(client, "c-basic", 5)

    r = _claim(client, "c-basic", "worker-1", 3)
    assert r.status_code == 200
    body = r.json()
    assert body["from_sequence"] == 0
    assert body["to_sequence"] == 3
    assert [e["sequence"] for e in body["events"]] == [0, 1, 2]
    assert [e["delta"] for e in body["events"]] == [1, 2, 3]
    assert body["lease_token"]
    assert body["expires_at"] > 0


def test_claim_skips_events_not_yet_applied_due_to_gap(client):
    # Sequences 1 and 2 arrive first and stay buffered (0 is missing).
    _post(client, "c-gap", "c-gap-e1", 1, 10)
    _post(client, "c-gap", "c-gap-e2", 2, 20)

    r = _claim(client, "c-gap", "worker", 10)
    assert r.status_code == 200
    body = r.json()
    # Nothing is applied yet: empty batch, but a lease still exists.
    assert body["events"] == []
    assert body["from_sequence"] == body["to_sequence"] == 0

    # Sequence 0 arrives; only it is applied (1 and 2 still pending the old gap?
    # actually all three become contiguous -> applied 0,1,2).
    _post(client, "c-gap", "c-gap-e0", 0, 5)

    # Expire the empty-batch lease, then re-claim.
    from app import consumers

    consumers._now_ms = lambda: body["expires_at"] + 1
    r = _claim(client, "c-gap", "worker", 10)
    assert [e["sequence"] for e in r.json()["events"]] == [0, 1, 2]


def test_reclaim_within_lease_returns_same_batch_and_token(client):
    _seed_applied_events(client, "c-same", 4)

    first = _claim(client, "c-same", "worker", 2).json()
    second = _claim(client, "c-same", "worker", 999).json()
    # Same batch, same token, same expiry, regardless of the new max_events.
    assert second == first


def test_ack_advances_cursor_and_next_claim_continues(client):
    _seed_applied_events(client, "c-ack", 5)

    batch = _claim(client, "c-ack", "worker", 2).json()
    r = _ack(client, "c-ack", "worker", batch["lease_token"])
    assert r.status_code == 200
    assert r.json()["cursor"] == 2

    next_batch = _claim(client, "c-ack", "worker", 10).json()
    assert next_batch["from_sequence"] == 2
    assert [e["sequence"] for e in next_batch["events"]] == [2, 3, 4]


def test_ack_with_wrong_token_returns_409_and_keeps_lease(client):
    _seed_applied_events(client, "c-wrong", 2)
    _claim(client, "c-wrong", "worker", 2)

    r = _ack(client, "c-wrong", "worker", "not-the-real-token")
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "lease_token_mismatch"

    # The valid lease is still usable and still covers the same batch.
    batch = _claim(client, "c-wrong", "worker", 2).json()
    assert [e["sequence"] for e in batch["events"]] == [0, 1]
    assert _ack(client, "c-wrong", "worker", batch["lease_token"]).status_code == 200


def test_ack_without_claim_returns_409(client):
    _seed_applied_events(client, "c-nolease", 1)
    r = _ack(client, "c-nolease", "fresh-worker", "anything")
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "lease_not_active"


def test_old_token_after_reclaim_is_rejected_with_409(client):
    _seed_applied_events(client, "c-old", 2)

    first = _claim(client, "c-old", "worker", 2).json()
    from app import consumers

    consumers._now_ms = lambda: first["expires_at"] + 1
    second = _claim(client, "c-old", "worker", 2).json()
    assert second["lease_token"] != first["lease_token"]

    # The old (expired) token must not advance the cursor.
    r = _ack(client, "c-old", "worker", first["lease_token"])
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "lease_token_mismatch"

    assert _ack(client, "c-old", "worker", second["lease_token"]).status_code == 200


def test_expired_lease_token_is_rejected_but_batch_reclaimable(client):
    _seed_applied_events(client, "c-exp", 2)
    batch = _claim(client, "c-exp", "worker", 2).json()

    from app import consumers

    consumers._now_ms = lambda: batch["expires_at"] + 1

    r = _ack(client, "c-exp", "worker", batch["lease_token"])
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "lease_expired"

    # Lease expired: the same batch is handed out again with a new token.
    again = _claim(client, "c-exp", "worker", 2).json()
    assert [e["sequence"] for e in again["events"]] == [0, 1]
    assert again["lease_token"] != batch["lease_token"]
    assert _ack(client, "c-exp", "worker", again["lease_token"]).json()["cursor"] == 2


def test_two_consumers_have_independent_cursors(client):
    _seed_applied_events(client, "c-two", 3)

    b1 = _claim(client, "c-two", "worker-a", 1).json()
    b2 = _claim(client, "c-two", "worker-b", 1).json()
    assert b1["events"][0]["sequence"] == 0
    assert b2["events"][0]["sequence"] == 0
    assert b1["lease_token"] != b2["lease_token"]

    assert _ack(client, "c-two", "worker-a", b1["lease_token"]).json()["cursor"] == 1
    assert _ack(client, "c-two", "worker-b", b2["lease_token"]).json()["cursor"] == 1


def test_claim_on_unknown_stream_returns_404(client):
    r = _claim(client, "no-such-stream", "worker", 10)
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "stream_not_found"


def test_ack_on_unknown_stream_returns_404(client):
    r = _ack(client, "no-such-stream", "worker", "token")
    assert r.status_code == 404


def test_claim_validation_rejects_bad_max_events(client):
    assert _claim(client, "c-val", "worker", 0).status_code == 422
    assert _claim(client, "c-val", "worker", -1).status_code == 422
    assert _claim(client, "c-val", "worker", True).status_code == 422
    assert client.post(
        "/streams/c-val/consumers/worker/claim", json={}
    ).status_code == 422
    assert client.post(
        "/streams/c-val/consumers/worker/ack", json={}
    ).status_code == 422


def test_concurrent_claims_never_produce_two_valid_leases(client):
    import threading

    _seed_applied_events(client, "c-conc", 10)
    tokens: list[str] = []

    def do_claim():
        r = _claim(client, "c-conc", "worker", 5)
        tokens.append(r.json()["lease_token"])

    threads = [threading.Thread(target=do_claim) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Every concurrent claimant observed the exact same valid lease.
    assert len(tokens) == 20
    assert len(set(tokens)) == 1

    batch = _claim(client, "c-conc", "worker", 5).json()
    assert [e["sequence"] for e in batch["events"]] == [0, 1, 2, 3, 4]
    assert _ack(client, "c-conc", "worker", tokens[0]).json()["cursor"] == 5


def test_events_arriving_after_claim_appear_after_ack(client):
    _seed_applied_events(client, "c-late", 2)
    batch = _claim(client, "c-late", "worker", 10).json()
    assert [e["sequence"] for e in batch["events"]] == [0, 1]

    # New events land while the batch is leased; they do not change the lease.
    _seed_applied_events(client, "c-late", 2, start=2)
    same = _claim(client, "c-late", "worker", 10).json()
    assert [e["sequence"] for e in same["events"]] == [0, 1]

    _ack(client, "c-late", "worker", batch["lease_token"])
    after = _claim(client, "c-late", "worker", 10).json()
    assert [e["sequence"] for e in after["events"]] == [2, 3]


def test_ack_of_empty_batch_advances_nothing(client):
    # Stream exists but no applied events.
    _post(client, "c-empty", "c-empty-e1", 1, 1)  # buffered, gap at 0
    batch = _claim(client, "c-empty", "worker", 5).json()
    assert batch["events"] == []
    assert batch["from_sequence"] == batch["to_sequence"] == 0

    r = _ack(client, "c-empty", "worker", batch["lease_token"])
    assert r.status_code == 200
    assert r.json()["cursor"] == 0


# --------------------------------------------------------------------------- #
# Stream forking
# --------------------------------------------------------------------------- #


def _fork(client, source_id, fork_id, target_id, through):
    return client.post(
        f"/streams/{source_id}/forks",
        json={
            "fork_id": fork_id,
            "target_stream_id": target_id,
            "through_sequence": through,
        },
    )


def _seed_for_fork(client, stream_id="f-src", deltas=(100, -30, 20, 10, 5)):
    for seq, delta in enumerate(deltas):
        assert _post(client, stream_id, f"{stream_id}-e{seq}", seq, delta).status_code in (
            200,
            201,
        )


def test_fork_copies_full_applied_prefix(client):
    _seed_for_fork(client, "f-src-full")
    r = _fork(client, "f-src-full", "fork-1", "f-dst", 5)
    assert r.status_code == 201
    assert r.json() == {
        "source_stream_id": "f-src-full",
        "target_stream_id": "f-dst",
        "through_sequence": 5,
        "balance": 105,
        "next_sequence": 5,
    }

    state = client.get("/streams/f-dst").json()
    assert state == {
        "stream_id": "f-dst",
        "balance": 105,
        "next_sequence": 5,
        "pending_sequences": [],
    }
    # The source is untouched.
    assert client.get("/streams/f-src-full").json()["balance"] == 105


def test_forked_stream_keeps_accepting_events(client):
    _seed_for_fork(client, "f-src-append")
    _fork(client, "f-src-append", "fork-1", "f-dst-append", 3)

    r = _post(client, "f-dst-append", "f-dst-append-e3", 3, 7)
    assert r.status_code == 201
    body = r.json()["stream"]
    assert body["next_sequence"] == 4
    assert body["balance"] == 97  # 100 - 30 + 20 + 7


def test_fork_partial_prefix(client):
    _seed_for_fork(client, "f-src-part")
    r = _fork(client, "f-src-part", "fork-part", "f-part", 2)
    assert r.status_code == 201
    body = r.json()
    assert body["through_sequence"] == 2
    assert body["balance"] == 70
    assert body["next_sequence"] == 2
    assert client.get("/streams/f-part").json()["pending_sequences"] == []


def test_fork_zero_through_sequence_copies_nothing(client):
    _seed_for_fork(client, "f-src-zero")
    r = _fork(client, "f-src-zero", "fork-zero", "f-zero", 0)
    assert r.status_code == 201
    assert r.json()["balance"] == 0
    assert r.json()["next_sequence"] == 0
    state = client.get("/streams/f-zero").json()
    assert state["balance"] == 0 and state["next_sequence"] == 0


def test_fork_skips_pending_unapplied_events(client):
    # Buffered pending events past the gap never enter the fork.
    _post(client, "f-gap-src", "e1", 1, 200)
    _post(client, "f-gap-src", "e2", 2, 50)
    _post(client, "f-gap-src", "e0", 0, 100)  # catch-up -> next_sequence=3
    _post(client, "f-gap-src", "e5", 5, 9)  # stays pending (gap at 3)

    r = _fork(client, "f-gap-src", "g", "f-gap-dst", 3)
    assert r.status_code == 201
    body = r.json()
    assert body["balance"] == 350 and body["next_sequence"] == 3
    assert client.get("/streams/f-gap-dst").json()["pending_sequences"] == []


def test_fork_on_unknown_source_returns_404(client):
    r = _fork(client, "no-such-source", "g", "f-404-dst", 0)
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "stream_not_found"
    # No target stream was created.
    assert client.get("/streams/f-404-dst").status_code == 404


def test_fork_into_existing_target_returns_409(client):
    _seed_for_fork(client, "f-src-taken")
    _post(client, "f-taken", "e0", 0, 1)
    r = _fork(client, "f-src-taken", "g", "f-taken", 1)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "target_stream_exists"


def test_fork_through_sequence_out_of_range_returns_422(client):
    _seed_for_fork(client, "f-src-range")
    r = _fork(client, "f-src-range", "g-too-far", "f-too-far", 6)
    assert r.status_code == 422
    assert r.json()["detail"]["code"] == "through_sequence_out_of_range"
    # The failed fork left nothing behind.
    assert client.get("/streams/f-too-far").status_code == 404


def test_fork_retry_with_same_parameters_is_idempotent(client):
    _seed_for_fork(client, "f-src-retry1")
    first = _fork(client, "f-src-retry1", "retry-fork", "f-retry", 4)
    assert first.status_code == 201

    second = _fork(client, "f-src-retry1", "retry-fork", "f-retry", 4)
    assert second.status_code == 200
    assert second.json() == first.json()

    # Exactly one copy of each event exists in the target.
    claim = _claim(client, "f-retry", "worker", 100).json()
    assert [e["sequence"] for e in claim["events"]] == [0, 1, 2, 3]


def test_fork_retry_with_different_parameters_returns_409(client):
    _seed_for_fork(client, "f-src-retry2")
    assert _fork(client, "f-src-retry2", "retry-fork", "f-retry2", 4).status_code == 201

    r = _fork(client, "f-src-retry2", "retry-fork", "f-retry2", 2)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "fork_replay_conflict"

    r = _fork(client, "f-src-retry2", "retry-fork", "f-retry2-other", 4)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "fork_replay_conflict"

    # No extra streams/events were created by the clashing retries.
    assert client.get("/streams/f-retry2-other").status_code == 404


def test_fork_validation_rejects_bad_payloads(client):
    _seed_for_fork(client, "f-src-val")
    url = "/streams/f-src-val/forks"
    assert client.post(url, json={"fork_id": "", "target_stream_id": "t", "through_sequence": 0}).status_code == 422
    assert client.post(url, json={"fork_id": "g", "target_stream_id": "", "through_sequence": 0}).status_code == 422
    assert client.post(url, json={"fork_id": "g", "target_stream_id": "t", "through_sequence": -1}).status_code == 422
    assert client.post(url, json={"fork_id": "g", "target_stream_id": "t", "through_sequence": True}).status_code == 422
    assert client.post(url, json={"fork_id": "g", "target_stream_id": "t"}).status_code == 422


def test_concurrent_forks_one_idempotent_fork(client):
    import threading

    _seed_for_fork(client, "f-src-conc1")
    statuses: list[int] = []

    def do_fork():
        r = _fork(client, "f-src-conc1", "concurrent-fork", "f-concurrent", 3)
        statuses.append(r.status_code)

    threads = [threading.Thread(target=do_fork) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(statuses) == [200] * 19 + [201]
    state = client.get("/streams/f-concurrent").json()
    assert state["next_sequence"] == 3 and state["balance"] == 90
    claim = _claim(client, "f-concurrent", "worker", 100).json()
    assert [e["sequence"] for e in claim["events"]] == [0, 1, 2]


def test_concurrent_forks_distinct_ids_same_target_never_half_build(client):
    import threading

    _seed_for_fork(client, "f-src-conc2")
    codes: list[str] = []

    def do_fork(i):
        r = _fork(client, "f-src-conc2", f"race-fork-{i}", "f-race2", 2)
        if r.status_code != 201:
            codes.append(r.json()["detail"]["code"])

    threads = [threading.Thread(target=do_fork, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(codes) == 19
    assert set(codes) == {"target_stream_exists"}
    state = client.get("/streams/f-race2").json()
    assert state["balance"] == 70 and state["next_sequence"] == 2
    claim = _claim(client, "f-race2", "worker", 100).json()
    assert [e["sequence"] for e in claim["events"]] == [0, 1]


# --------------------------------------------------------------------------- #
# Cross-stream atomic transfers
# --------------------------------------------------------------------------- #


def _transfer(client, transfer_id, source_id, target_id, amount):
    return client.post(
        "/transfers",
        json={
            "transfer_id": transfer_id,
            "source_stream_id": source_id,
            "target_stream_id": target_id,
            "amount": amount,
        },
    )


def _seed_transfer_streams(client, source="t-src", target="t-dst", source_balance=100, target_balance=10):
    r = _post(client, source, f"{source}-e0", 0, source_balance)
    assert r.status_code in (200, 201)
    r = _post(client, target, f"{target}-e0", 0, target_balance)
    assert r.status_code in (200, 201)


def test_transfer_moves_amount_atomically(client):
    _seed_transfer_streams(client)

    r = _transfer(client, "xfer-1", "t-src", "t-dst", 40)
    assert r.status_code == 201
    body = r.json()
    assert body["transfer_id"] == "xfer-1"
    assert body["amount"] == 40
    # Debit/credit events were appended at each side's current next_sequence.
    assert body["source_sequence"] == 1
    assert body["target_sequence"] == 1
    assert body["source"] == {
        "stream_id": "t-src",
        "balance": 60,
        "next_sequence": 2,
        "pending_sequences": [],
    }
    assert body["target"] == {
        "stream_id": "t-dst",
        "balance": 50,
        "next_sequence": 2,
        "pending_sequences": [],
    }

    # Both streams reflect the transfer and the events are really there.
    assert client.get("/streams/t-src").json()["balance"] == 60
    assert client.get("/streams/t-dst").json()["balance"] == 50
    src_events = _claim(client, "t-src", "worker", 10).json()["events"]
    dst_events = _claim(client, "t-dst", "worker", 10).json()["events"]
    assert [(e["sequence"], e["delta"]) for e in src_events] == [(0, 100), (1, -40)]
    assert [(e["sequence"], e["delta"]) for e in dst_events] == [(0, 10), (1, 40)]


def test_transfer_appends_at_current_next_sequence(client):
    _seed_transfer_streams(client, source="t-seq-src", target="t-seq-dst")
    _post(client, "t-seq-src", "t-seq-src-e1", 1, -20)
    _post(client, "t-seq-dst", "t-seq-dst-e1", 1, 5)
    _post(client, "t-seq-dst", "t-seq-dst-e2", 2, 5)

    r = _transfer(client, "xfer-seq", "t-seq-src", "t-seq-dst", 30)
    assert r.status_code == 201
    body = r.json()
    assert body["source_sequence"] == 2
    assert body["target_sequence"] == 3
    assert body["source"]["balance"] == 50  # 100 - 20 - 30
    assert body["target"]["balance"] == 50  # 10 + 5 + 5 + 30


def test_transfer_unblocks_pending_events_via_contiguous_catch_up(client):
    # Source has a buffered event at sequence 2 (gap at 1).
    _post(client, "t-gap-src", "t-gap-src-e0", 0, 100)
    _post(client, "t-gap-src", "t-gap-src-e2", 2, 7)
    _post(client, "t-gap-dst", "t-gap-dst-e0", 0, 0)

    r = _transfer(client, "xfer-gap", "t-gap-src", "t-gap-dst", 10)
    assert r.status_code == 201
    body = r.json()
    # The debit event fills the gap at 1; catch-up also applies the buffered 2.
    assert body["source_sequence"] == 1
    assert body["source"]["balance"] == 97  # 100 - 10 + 7
    assert body["source"]["next_sequence"] == 3
    assert body["source"]["pending_sequences"] == []
    assert client.get("/streams/t-gap-src").json()["pending_sequences"] == []


def test_transfer_retry_with_same_parameters_is_idempotent(client):
    _seed_transfer_streams(client, source="t-re-src", target="t-re-dst")

    first = _transfer(client, "xfer-retry", "t-re-src", "t-re-dst", 25)
    assert first.status_code == 201

    second = _transfer(client, "xfer-retry", "t-re-src", "t-re-dst", 25)
    assert second.status_code == 200
    assert second.json() == first.json()

    # No double debit: balances and sequences moved exactly once.
    assert client.get("/streams/t-re-src").json()["balance"] == 75
    assert client.get("/streams/t-re-dst").json()["balance"] == 35
    assert client.get("/streams/t-re-src").json()["next_sequence"] == 2
    assert client.get("/streams/t-re-dst").json()["next_sequence"] == 2


def test_transfer_retry_with_different_parameters_returns_409(client):
    _seed_transfer_streams(client, source="t-drift-src", target="t-drift-dst")
    assert _transfer(client, "xfer-drift", "t-drift-src", "t-drift-dst", 10).status_code == 201

    r = _transfer(client, "xfer-drift", "t-drift-src", "t-drift-dst", 11)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "transfer_replay_conflict"

    r = _transfer(client, "xfer-drift", "t-drift-src", "t-drift-dst2", 10)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "transfer_replay_conflict"

    r = _transfer(client, "xfer-drift", "t-drift-dst", "t-drift-src", 10)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "transfer_replay_conflict"

    # The clashing retries changed nothing.
    assert client.get("/streams/t-drift-src").json()["balance"] == 90
    assert client.get("/streams/t-drift-dst").json()["balance"] == 20


def test_transfer_unknown_stream_returns_404_and_creates_nothing(client):
    _seed_transfer_streams(client, source="t-404-src", target="t-404-dst")

    r = _transfer(client, "xfer-404a", "t-404-src", "no-such-target", 10)
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "stream_not_found"

    r = _transfer(client, "xfer-404b", "no-such-source", "t-404-dst", 10)
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "stream_not_found"

    # Unlike event submission, transfers never create streams, and the failed
    # attempts left no events or balance changes behind.
    assert client.get("/streams/no-such-target").status_code == 404
    assert client.get("/streams/no-such-source").status_code == 404
    assert client.get("/streams/t-404-src").json() == {
        "stream_id": "t-404-src",
        "balance": 100,
        "next_sequence": 1,
        "pending_sequences": [],
    }
    assert client.get("/streams/t-404-dst").json()["balance"] == 10


def test_transfer_insufficient_funds_returns_409_and_leaves_nothing(client):
    _seed_transfer_streams(client, source="t-poor-src", target="t-poor-dst")

    r = _transfer(client, "xfer-poor", "t-poor-src", "t-poor-dst", 101)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "insufficient_funds"

    # No events, no balance changes, no transfer record: the same transfer_id
    # can be reused once the source has been topped up.
    assert client.get("/streams/t-poor-src").json()["next_sequence"] == 1
    assert client.get("/streams/t-poor-dst").json()["next_sequence"] == 1
    _post(client, "t-poor-src", "t-poor-src-e1", 1, 50)
    r = _transfer(client, "xfer-poor", "t-poor-src", "t-poor-dst", 101)
    assert r.status_code == 201
    assert r.json()["source"]["balance"] == 49
    assert r.json()["target"]["balance"] == 111


def test_transfer_equal_amount_to_balance_is_allowed(client):
    _seed_transfer_streams(client, source="t-all-src", target="t-all-dst")
    r = _transfer(client, "xfer-all", "t-all-src", "t-all-dst", 100)
    assert r.status_code == 201
    assert r.json()["source"]["balance"] == 0
    assert r.json()["target"]["balance"] == 110


def test_transfer_validation_rejects_bad_payloads(client):
    _seed_transfer_streams(client, source="t-val-src", target="t-val-dst")

    def xfer(**overrides):
        body = {
            "transfer_id": "xfer-val",
            "source_stream_id": "t-val-src",
            "target_stream_id": "t-val-dst",
            "amount": 1,
        }
        body.update(overrides)
        return client.post("/transfers", json=body)

    assert xfer(transfer_id="").status_code == 422
    assert xfer(source_stream_id="").status_code == 422
    assert xfer(target_stream_id="").status_code == 422
    assert xfer(amount=0).status_code == 422
    assert xfer(amount=-5).status_code == 422
    assert xfer(amount=True).status_code == 422
    assert xfer(amount="lots").status_code == 422
    # Source and target must be different streams.
    assert xfer(source_stream_id="t-val-src", target_stream_id="t-val-src").status_code == 422
    # Missing fields.
    assert client.post("/transfers", json={}).status_code == 422
    # Nothing was written by the rejected payloads.
    assert client.get("/streams/t-val-src").json()["balance"] == 100
    assert client.get("/streams/t-val-dst").json()["balance"] == 10


def test_concurrent_transfer_retries_commit_exactly_once(client):
    import threading

    _seed_transfer_streams(client, source="t-conc-src", target="t-conc-dst")
    statuses: list[int] = []
    bodies: list[dict] = []

    def do_transfer():
        r = _transfer(client, "xfer-conc", "t-conc-src", "t-conc-dst", 30)
        statuses.append(r.status_code)
        bodies.append(r.json())

    threads = [threading.Thread(target=do_transfer) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Exactly one transfer committed; every retry returned the original result.
    assert sorted(statuses) == [200] * 19 + [201]
    first = next(b for s, b in zip(statuses, bodies) if s == 201)
    assert all(b == first for b in bodies)
    assert client.get("/streams/t-conc-src").json()["balance"] == 70
    assert client.get("/streams/t-conc-dst").json()["balance"] == 40
    assert client.get("/streams/t-conc-src").json()["next_sequence"] == 2
    assert client.get("/streams/t-conc-dst").json()["next_sequence"] == 2


def test_concurrent_transfers_from_same_source_never_overdraw(client):
    import threading

    _seed_transfer_streams(client, source="t-race-src", target="t-race-dst1")
    _post(client, "t-race-dst2", "t-race-dst2-e0", 0, 0)
    outcomes: list[tuple[int, str]] = []

    def do_transfer(i):
        r = _transfer(client, f"xfer-race-{i}", "t-race-src", f"t-race-dst{i}", 70)
        code = r.json()["detail"]["code"] if r.status_code != 201 else "created"
        outcomes.append((r.status_code, code))

    threads = [threading.Thread(target=do_transfer, args=(i,)) for i in (1, 2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # The source holds 100: exactly one of the two 70-unit transfers commits.
    assert sorted(outcomes) == [(201, "created"), (409, "insufficient_funds")]
    assert client.get("/streams/t-race-src").json()["balance"] == 30
    moved = client.get("/streams/t-race-dst1").json()["balance"] + client.get(
        "/streams/t-race-dst2"
    ).json()["balance"]
    assert moved == 10 + 70  # dst1 started at 10, dst2 at 0


def test_transfer_events_are_visible_to_consumers_and_forks(client):
    _seed_transfer_streams(client, source="t-vis-src", target="t-vis-dst")
    assert _transfer(client, "xfer-vis", "t-vis-src", "t-vis-dst", 40).status_code == 201

    # Consumers see the debit/credit events like any other applied event.
    src = _claim(client, "t-vis-src", "w", 10).json()
    dst = _claim(client, "t-vis-dst", "w", 10).json()
    assert [e["delta"] for e in src["events"]] == [100, -40]
    assert [e["delta"] for e in dst["events"]] == [10, 40]

    # A fork of the target includes the credited event.
    r = _fork(client, "t-vis-dst", "fork-after-transfer", "t-vis-fork", 2)
    assert r.status_code == 201
    assert r.json()["balance"] == 50
