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


def _transfer(client, transfer_id, source, target, amount):
    return client.post(
        "/transfers",
        json={
            "transfer_id": transfer_id,
            "source_stream_id": source,
            "target_stream_id": target,
            "amount": amount,
        },
    )


def _seed_balance(client, stream_id, amount):
    """Create a stream whose sole applied event credits ``amount`` at sequence 0."""
    r = _post(client, stream_id, f"{stream_id}-seed", 0, amount)
    assert r.status_code in (200, 201)


def test_transfer_moves_amount_and_reports_both_sides(client):
    _seed_balance(client, "t-a", 100)
    _seed_balance(client, "t-b", 20)

    r = _transfer(client, "txn-1", "t-a", "t-b", 30)
    assert r.status_code == 201
    body = r.json()
    assert body["replayed"] is False
    assert body["amount"] == 30
    assert body["source"] == {
        "stream_id": "t-a",
        "event_id": "transfer:txn-1:debit",
        "sequence": 1,
        "state": {
            "stream_id": "t-a",
            "balance": 70,
            "next_sequence": 2,
            "pending_sequences": [],
        },
    }
    assert body["target"] == {
        "stream_id": "t-b",
        "event_id": "transfer:txn-1:credit",
        "sequence": 1,
        "state": {
            "stream_id": "t-b",
            "balance": 50,
            "next_sequence": 2,
            "pending_sequences": [],
        },
    }

    # The synthetic events are visible through the ordinary stream endpoint.
    assert client.get("/streams/t-a").json()["balance"] == 70
    assert client.get("/streams/t-b").json()["balance"] == 50


def test_transfer_events_are_consumable_like_normal_events(client):
    _seed_balance(client, "t-consume-a", 100)
    _seed_balance(client, "t-consume-b", 0)
    r = _transfer(client, "txn-consume", "t-consume-a", "t-consume-b", 40)
    assert r.status_code == 201

    batch_a = _claim(client, "t-consume-a", "worker", 100).json()
    assert [(e["sequence"], e["delta"]) for e in batch_a["events"]] == [(0, 100), (1, -40)]
    batch_b = _claim(client, "t-consume-b", "worker", 100).json()
    assert [(e["sequence"], e["delta"]) for e in batch_b["events"]] == [(0, 0), (1, 40)]


def test_transfer_exact_balance_succeeds(client):
    _seed_balance(client, "t-exact", 50)
    _seed_balance(client, "t-exact-dst", 0)
    r = _transfer(client, "txn-exact", "t-exact", "t-exact-dst", 50)
    assert r.status_code == 201
    assert r.json()["source"]["state"]["balance"] == 0
    assert r.json()["target"]["state"]["balance"] == 50


def test_transfer_insufficient_funds_returns_422_and_changes_nothing(client):
    _seed_balance(client, "t-poor", 10)
    _seed_balance(client, "t-poor-dst", 0)
    r = _transfer(client, "txn-poor", "t-poor", "t-poor-dst", 11)
    assert r.status_code == 422
    assert r.json()["detail"]["code"] == "insufficient_funds"

    # No events, no balance movement, no transfer record.
    assert client.get("/streams/t-poor").json() == {
        "stream_id": "t-poor",
        "balance": 10,
        "next_sequence": 1,
        "pending_sequences": [],
    }
    assert client.get("/streams/t-poor-dst").json()["balance"] == 0
    # The same transfer_id is still usable after the failed attempt.
    assert _transfer(client, "txn-poor", "t-poor", "t-poor-dst", 5).status_code == 201


def test_transfer_missing_stream_returns_404_and_creates_nothing(client):
    _seed_balance(client, "t-existing", 10)
    r = _transfer(client, "txn-404-a", "t-missing-src", "t-existing", 1)
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "source_stream_not_found"
    assert client.get("/streams/t-missing-src").status_code == 404

    r = _transfer(client, "txn-404-b", "t-existing", "t-missing-dst", 1)
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "target_stream_not_found"
    assert client.get("/streams/t-missing-dst").status_code == 404
    # Nothing was debited from the existing stream.
    assert client.get("/streams/t-existing").json()["balance"] == 10


def test_transfer_replay_with_same_parameters_returns_original_result(client):
    _seed_balance(client, "t-rep-a", 100)
    _seed_balance(client, "t-rep-b", 0)
    first = _transfer(client, "txn-rep", "t-rep-a", "t-rep-b", 30)
    assert first.status_code == 201
    first_body = first.json()

    second = _transfer(client, "txn-rep", "t-rep-a", "t-rep-b", 30)
    assert second.status_code == 200
    second_body = second.json()
    assert second_body["replayed"] is True
    # Original event coordinates are preserved ...
    assert (
        second_body["source"]["sequence"] == first_body["source"]["sequence"] == 1
    )
    assert (
        second_body["target"]["sequence"] == first_body["target"]["sequence"] == 1
    )
    assert second_body["source"]["event_id"] == "transfer:txn-rep:debit"
    assert second_body["target"]["event_id"] == "transfer:txn-rep:credit"

    # ... and the source was not debited a second time.
    assert client.get("/streams/t-rep-a").json()["balance"] == 70
    assert client.get("/streams/t-rep-b").json()["balance"] == 30
    claim = _claim(client, "t-rep-a", "worker", 100).json()
    assert [e["sequence"] for e in claim["events"]] == [0, 1]


def test_transfer_replay_after_other_events_reports_current_state(client):
    _seed_balance(client, "t-late-a", 100)
    _seed_balance(client, "t-late-b", 0)
    first = _transfer(client, "txn-late", "t-late-a", "t-late-b", 30).json()

    # More events land on both streams; the replay keeps the original event
    # sequences but returns the latest stream state.
    _post(client, "t-late-a", "a-extra", 2, -5)
    _post(client, "t-late-b", "b-extra", 2, 7)

    second = _transfer(client, "txn-late", "t-late-a", "t-late-b", 30).json()
    assert second["source"]["sequence"] == first["source"]["sequence"] == 1
    assert second["target"]["sequence"] == first["target"]["sequence"] == 1
    assert second["source"]["state"]["balance"] == 65
    assert second["source"]["state"]["next_sequence"] == 3
    assert second["target"]["state"]["balance"] == 37
    assert second["target"]["state"]["next_sequence"] == 3


def test_transfer_replay_with_different_parameters_returns_409(client):
    _seed_balance(client, "t-conf-a", 100)
    _seed_balance(client, "t-conf-b", 100)
    _seed_balance(client, "t-conf-c", 100)
    assert _transfer(client, "txn-conf", "t-conf-a", "t-conf-b", 10).status_code == 201

    r = _transfer(client, "txn-conf", "t-conf-a", "t-conf-b", 20)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "transfer_replay_conflict"

    r = _transfer(client, "txn-conf", "t-conf-c", "t-conf-b", 10)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "transfer_replay_conflict"

    r = _transfer(client, "txn-conf", "t-conf-a", "t-conf-c", 10)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "transfer_replay_conflict"

    # The clashing retries moved no extra money.
    assert client.get("/streams/t-conf-a").json()["balance"] == 90
    assert client.get("/streams/t-conf-b").json()["balance"] == 110
    assert client.get("/streams/t-conf-c").json()["balance"] == 100


def test_concurrent_transfer_retries_commit_exactly_once(client):
    import threading

    _seed_balance(client, "t-race-a", 100)
    _seed_balance(client, "t-race-b", 0)
    statuses: list[int] = []

    def do_transfer():
        r = _transfer(client, "txn-race", "t-race-a", "t-race-b", 40)
        statuses.append(r.status_code)

    threads = [threading.Thread(target=do_transfer) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(statuses) == [200] * 19 + [201]
    assert client.get("/streams/t-race-a").json()["balance"] == 60
    assert client.get("/streams/t-race-b").json()["balance"] == 40
    claim_a = _claim(client, "t-race-a", "worker", 100).json()
    claim_b = _claim(client, "t-race-b", "worker", 100).json()
    assert [e["sequence"] for e in claim_a["events"]] == [0, 1]
    assert [e["sequence"] for e in claim_b["events"]] == [0, 1]


def test_concurrent_distinct_transfers_serialise_and_conserve_money(client):
    import threading

    _seed_balance(client, "t-many-a", 100)
    _seed_balance(client, "t-many-b", 100)

    def do_transfer(i):
        _transfer(client, f"txn-many-{i}", "t-many-a", "t-many-b", 1)

    threads = [threading.Thread(target=do_transfer, args=(i,)) for i in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Every one-unit transfer landed exactly once; total money is conserved.
    state_a = client.get("/streams/t-many-a").json()
    state_b = client.get("/streams/t-many-b").json()
    assert state_a["balance"] == 50 and state_a["next_sequence"] == 51
    assert state_b["balance"] == 150 and state_b["next_sequence"] == 51
    assert state_a["balance"] + state_b["balance"] == 200


def test_transfer_unblocks_buffered_events_on_both_sides(client):
    # Source: sequence 1 is parked behind a gap at 0; the transfer lands at
    # next_sequence 0 and must unblock the buffered event via contiguous catch-up.
    _post(client, "t-gap-a", "a-e1", 1, -30)  # buffered, balance 0, next 0
    _post(client, "t-gap-b", "b-e1", 1, 5)  # buffered, balance 0, next 0
    # Seed applied prefixes so both streams exist with spendable balances.
    _post(client, "t-gap-a", "a-e0", 0, 100)  # catch-up -> balance 70, next 2
    _post(client, "t-gap-b", "b-e0", 0, 0)  # catch-up -> balance 5, next 2
    # Park another buffered run behind sequence 3 on both sides.
    _post(client, "t-gap-a", "a-e4", 4, 2)
    _post(client, "t-gap-b", "b-e4", 4, 1)

    r = _transfer(client, "txn-gap", "t-gap-a", "t-gap-b", 20)
    assert r.status_code == 201
    body = r.json()
    # Transfer events appended at the current next_sequence (2); sequence 3 is
    # still a gap on both sides, so [4] stays buffered.
    assert body["source"]["sequence"] == 2
    assert body["target"]["sequence"] == 2
    assert body["source"]["state"] == {
        "stream_id": "t-gap-a",
        "balance": 50,  # 70 - 20
        "next_sequence": 3,
        "pending_sequences": [4],
    }
    assert body["target"]["state"] == {
        "stream_id": "t-gap-b",
        "balance": 25,  # 5 + 20
        "next_sequence": 3,
        "pending_sequences": [4],
    }

    # Closing the gap applies the still-buffered event normally.
    _post(client, "t-gap-a", "a-e3", 3, 1)
    _post(client, "t-gap-b", "b-e3", 3, 1)
    assert client.get("/streams/t-gap-a").json()["balance"] == 53
    assert client.get("/streams/t-gap-b").json()["balance"] == 27


def test_transfer_validation_rejects_bad_payloads(client):
    _seed_balance(client, "t-val", 10)
    base = {"transfer_id": "g", "source_stream_id": "t-val", "target_stream_id": "x", "amount": 1}

    r = client.post("/transfers", json={**base, "transfer_id": ""})
    assert r.status_code == 422
    r = client.post("/transfers", json={**base, "source_stream_id": ""})
    assert r.status_code == 422
    r = client.post("/transfers", json={**base, "target_stream_id": ""})
    assert r.status_code == 422
    for bad_amount in (0, -1, "5", True):
        assert client.post("/transfers", json={**base, "amount": bad_amount}).status_code == 422
    # Source and target must be different streams.
    r = client.post(
        "/transfers",
        json={**base, "target_stream_id": "t-val"},
    )
    assert r.status_code == 422
    # Missing fields.
    assert client.post("/transfers", json={"transfer_id": "g"}).status_code == 422
    assert client.post("/transfers", json={}).status_code == 422


def test_transfer_coexists_with_forks_and_consumers(client):
    _seed_balance(client, "t-mix-a", 100)
    _seed_balance(client, "t-mix-b", 0)
    assert _transfer(client, "txn-mix-2", "t-mix-a", "t-mix-b", 40).status_code == 201

    # Fork the source: both seed (0) and debit (1) events land in the copy.
    r = _fork(client, "t-mix-a", "fork-mix", "t-mix-fork", 2)
    assert r.status_code == 201
    assert r.json()["balance"] == 60 and r.json()["next_sequence"] == 2

    # Consumers see transfer events in order, and ack still works.
    batch = _claim(client, "t-mix-a", "worker", 10).json()
    assert [(e["event_id"], e["sequence"], e["delta"]) for e in batch["events"]] == [
        ("t-mix-a-seed", 0, 100),
        ("transfer:txn-mix-2:debit", 1, -40),
    ]
    assert _ack(client, "t-mix-a", "worker", batch["lease_token"]).json()["cursor"] == 2
    assert _claim(client, "t-mix-a", "worker", 10).json()["events"] == []
