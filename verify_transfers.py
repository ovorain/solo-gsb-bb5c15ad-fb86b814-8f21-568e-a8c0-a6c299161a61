"""Stdlib-only smoke test for cross-stream transfers (run without FastAPI installed)."""

import os
import tempfile
import threading
import traceback

tmp = tempfile.mkdtemp()
os.environ["DATABASE_PATH"] = os.path.join(tmp, "transfers.db")

from app import db, repo, consumers, transfers  # noqa: E402

db.initialise()

failures = []


def check(name, cond, extra=""):
    print(f"{'PASS' if cond else 'FAIL'}: {name} {extra}")
    if not cond:
        failures.append(name)


# Two existing streams: src has 100 (one applied event), dst has 10.
repo.submit_event("src", "src-e0", 0, 100)
repo.submit_event("dst", "dst-e0", 0, 10)

# 1. basic transfer: debit/credit appended at each side's next_sequence
t = transfers.execute_transfer("x-1", "src", "dst", 40)
check("transfer created", not t.replayed, str(t))
check("transfer sequences", t.source_sequence == 1 and t.target_sequence == 1,
      str((t.source_sequence, t.target_sequence)))
check("transfer states", t.source.balance == 60 and t.source.next_sequence == 2
      and t.target.balance == 50 and t.target.next_sequence == 2
      and t.source.pending_sequences == [] and t.target.pending_sequences == [],
      str((t.source, t.target)))
src, dst = repo.get_stream_state("src"), repo.get_stream_state("dst")
check("streams reflect transfer", src.balance == 60 and src.next_sequence == 2
      and dst.balance == 50 and dst.next_sequence == 2, str((src, dst)))

# the debit/credit events are stored and applied like regular events
with db.read_connection() as conn:
    rows = conn.execute(
        "SELECT stream_id, event_id, sequence, delta, applied FROM events "
        "WHERE event_id LIKE 'transfer:%' ORDER BY stream_id"
    ).fetchall()
check("transfer events stored applied",
      [tuple(r) for r in rows] == [
          ("dst", "transfer:x-1:credit", 1, 40, 1),
          ("src", "transfer:x-1:debit", 1, -40, 1),
      ], str([tuple(r) for r in rows]))

# 2. idempotent replay: same transfer_id + same params -> original result
t2 = transfers.execute_transfer("x-1", "src", "dst", 40)
check("replay returns original result", t2.replayed and t2.as_dict() == t.as_dict(),
      str(t2))
check("replay did not move funds again",
      repo.get_stream_state("src").balance == 60
      and repo.get_stream_state("dst").balance == 50)

# 3. same transfer_id with different parameters -> 409
for args in [("x-1", "src", "dst", 41), ("x-1", "src", "other", 40), ("x-1", "dst", "src", 40)]:
    try:
        transfers.execute_transfer(*args)
        check(f"replay drift {args[1:]} -> 409", False)
    except transfers.TransferError as e:
        check(f"replay drift {args[1:]} -> 409",
              e.code == "transfer_replay_conflict" and e.status_code == 409, str(e.code))

# 4. unknown streams -> 404, nothing created/changed
for args in [("x-404a", "src", "ghost", 1), ("x-404b", "ghost", "dst", 1)]:
    try:
        transfers.execute_transfer(*args)
        check(f"unknown stream {args[1:]} -> 404", False)
    except transfers.TransferError as e:
        check(f"unknown stream {args[1:]} -> 404",
              e.code == "stream_not_found" and e.status_code == 404, str(e.code))
check("ghost streams not created", repo.get_stream_state("ghost") is None)
check("balances untouched by 404s", repo.get_stream_state("src").balance == 60
      and repo.get_stream_state("dst").balance == 50)

# 5. insufficient funds -> 409, atomic rollback, transfer_id stays reusable
try:
    transfers.execute_transfer("x-poor", "src", "dst", 999)
    check("insufficient funds -> 409", False)
except transfers.TransferError as e:
    check("insufficient funds -> 409",
          e.code == "insufficient_funds" and e.status_code == 409, str(e.code))
src = repo.get_stream_state("src")
check("failed transfer left nothing", src.balance == 60 and src.next_sequence == 2
      and repo.get_stream_state("dst").balance == 50, str(src))
t3 = transfers.execute_transfer("x-poor", "src", "dst", 10)
check("failed transfer_id reusable", not t3.replayed and t3.source.balance == 50
      and t3.target.balance == 60, str(t3))

# 6. draining the full balance is allowed
t4 = transfers.execute_transfer("x-all", "src", "dst", 50)
check("full-balance transfer", t4.source.balance == 0 and t4.target.balance == 110,
      str(t4))

# 7. transfer un-blocks buffered events via the contiguous catch-up
repo.submit_event("gap-src", "g0", 0, 100)
repo.submit_event("gap-src", "g2", 2, 7)  # buffered: gap at sequence 1
t5 = transfers.execute_transfer("x-gap", "gap-src", "dst", 10)
check("transfer fills gap and catches up", t5.source_sequence == 1
      and t5.source.balance == 97 and t5.source.next_sequence == 3
      and t5.source.pending_sequences == [], str(t5))

# 8. consumers see the transfer events like any other applied event
c = consumers.claim("dst", "w", 100)
check("claim sees credit events",
      [(e.sequence, e.delta) for e in c.events]
      == [(0, 10), (1, 40), (2, 10), (3, 50), (4, 10)], str(c))

# 9. concurrency: 20 threads, same transfer_id/params -> exactly one commit
results, errors = [], []
latch = threading.Barrier(20)


def same_transfer():
    try:
        latch.wait()
        results.append(transfers.execute_transfer("x-conc", "dst", "src", 5))
    except Exception:
        errors.append(traceback.format_exc())


threads = [threading.Thread(target=same_transfer) for _ in range(20)]
for th in threads:
    th.start()
for th in threads:
    th.join()
check("concurrent retries no errors", not errors, str(errors[:1]))
check("concurrent retries one winner",
      len(results) == 20 and sum(1 for r in results if not r.replayed) == 1
      and sum(1 for r in results if r.replayed) == 19,
      f"replayed={sum(r.replayed for r in results)}")
check("concurrent retries identical result",
      len({r.as_dict()["source_sequence"] for r in results}) == 1
      and len({r.as_dict()["target_sequence"] for r in results}) == 1)
# Funds moved exactly once: src=100-40-10-50+5=5, dst=10+40+10+50+10-5=115.
src_state = repo.get_stream_state("src")
dst_state = repo.get_stream_state("dst")
check("exact final balances", src_state.balance == 5 and dst_state.balance == 115,
      str((src_state, dst_state)))

# 10. concurrency: two transfers racing for the same insufficient balance
repo.submit_event("race-src", "r0", 0, 100)
outcomes, race_errors = [], []
latch2 = threading.Barrier(2)


def race_transfer(i):
    try:
        latch2.wait()
        transfers.execute_transfer(f"x-race-{i}", "race-src", "dst", 70)
        outcomes.append("created")
    except transfers.TransferError as e:
        outcomes.append(e.code)
    except Exception:
        race_errors.append(traceback.format_exc())


threads = [threading.Thread(target=race_transfer, args=(i,)) for i in range(2)]
for th in threads:
    th.start()
for th in threads:
    th.join()
check("racing transfers no unexpected errors", not race_errors, str(race_errors[:1]))
check("racing transfers exactly one commits",
      sorted(outcomes) == ["created", "insufficient_funds"], str(outcomes))
check("no overdraw", repo.get_stream_state("race-src").balance == 30,
      str(repo.get_stream_state("race-src")))

print()
if failures:
    print(f"{len(failures)} FAILURES: {failures}")
    raise SystemExit(1)
print("ALL TRANSFER CHECKS PASSED")
