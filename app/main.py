"""FastAPI application: out-of-order event submission and stream queries."""

from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

from . import consumers, db, forks, repo


@asynccontextmanager
async def lifespan(_: FastAPI):
    db.initialise()
    yield


app = FastAPI(
    title="Out-of-Order Event Merge API",
    version="1.0.0",
    description=(
        "Submit delta events with per-stream sequence numbers. Contiguous events "
        "are folded into the stream balance; gaps are buffered and caught up "
        "atomically when the missing event arrives."
    ),
    lifespan=lifespan,
)


class EventIn(BaseModel):
    event_id: str = Field(..., min_length=1, description="Client-assigned unique event id")
    sequence: int = Field(..., ge=0, description="Monotonic sequence within the stream, starting at 0")
    delta: int = Field(..., description="Integer amount added to the balance when the event is applied")

    @field_validator("sequence", "delta", mode="before")
    @classmethod
    def _reject_bool(cls, value):
        # In lax mode pydantic accepts bool as int; JSON booleans are not integers here.
        if isinstance(value, bool):
            raise ValueError("must be an integer, not a boolean")
        return value

    model_config = {"json_schema_extra": {"examples": [{"event_id": "evt-1", "sequence": 0, "delta": 100}]}}


class StreamStateOut(BaseModel):
    stream_id: str
    balance: int
    next_sequence: int
    pending_sequences: list[int]


class EventAck(BaseModel):
    status: str
    replayed: bool
    stream: StreamStateOut


class ClaimIn(BaseModel):
    max_events: int = Field(
        ..., ge=1, le=10_000, description="Maximum number of events in this batch"
    )

    @field_validator("max_events", mode="before")
    @classmethod
    def _reject_bool(cls, value):
        # In lax mode pydantic accepts bool as int; JSON booleans are not integers here.
        if isinstance(value, bool):
            raise ValueError("must be an integer, not a boolean")
        return value

    model_config = {"json_schema_extra": {"examples": [{"max_events": 100}]}}


class ClaimedEventOut(BaseModel):
    event_id: str
    sequence: int
    delta: int


class ClaimOut(BaseModel):
    stream_id: str
    consumer_id: str
    lease_token: str
    expires_at: int = Field(..., description="POSIX milliseconds; the lease is valid until then")
    from_sequence: int
    to_sequence: int = Field(..., description="Exclusive upper bound of the claimed batch")
    events: list[ClaimedEventOut]


class AckIn(BaseModel):
    lease_token: str = Field(..., min_length=1)

    model_config = {"json_schema_extra": {"examples": [{"lease_token": "<token from claim>"}]}}


class AckOut(BaseModel):
    status: str
    stream_id: str
    consumer_id: str
    cursor: int = Field(..., description="New cursor sequence (next event this consumer will receive)")


class ForkIn(BaseModel):
    fork_id: str = Field(..., min_length=1, description="Client-assigned idempotency key, unique per source stream")
    target_stream_id: str = Field(..., min_length=1, description="Stream created by the fork; must not already exist")
    through_sequence: int = Field(..., ge=0, description="Exclusive upper bound; must be <= the source next_sequence")

    @field_validator("through_sequence", mode="before")
    @classmethod
    def _reject_bool(cls, value):
        # In lax mode pydantic accepts bool as int; JSON booleans are not integers here.
        if isinstance(value, bool):
            raise ValueError("must be an integer, not a boolean")
        return value

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"fork_id": "fork-1", "target_stream_id": "acct-1-clone", "through_sequence": 2}
            ]
        }
    }


class ForkOut(BaseModel):
    source_stream_id: str
    target_stream_id: str
    through_sequence: int = Field(..., description="Exclusive upper bound of the copied prefix")
    balance: int = Field(..., description="Initial balance of the target stream")
    next_sequence: int = Field(..., description="Initial next_sequence of the target stream")


@app.get("/health", tags=["meta"])
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/streams/{stream_id}/events",
    response_model=EventAck,
    status_code=201,
    responses={200: {"model": EventAck, "description": "Idempotent replay of an existing event"}},
    tags=["events"],
    summary="Submit one event for a stream",
)
def submit_event(stream_id: str, event: EventIn):
    try:
        state, replayed = repo.submit_event(
            stream_id, event.event_id, event.sequence, event.delta
        )
    except repo.ConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": exc.message},
        )
    except sqlite3.IntegrityError as exc:
        # Defence in depth: the UNIQUE constraints map to a 409 as well.
        raise HTTPException(
            status_code=409,
            detail={"code": "integrity_conflict", "message": str(exc)},
        )

    payload = EventAck(
        status="ok", replayed=replayed, stream=StreamStateOut(**state.as_dict())
    ).model_dump()
    # Idempotent replays return 200; first-time inserts return 201.
    return JSONResponse(payload, status_code=200 if replayed else 201)


@app.get(
    "/streams/{stream_id}",
    response_model=StreamStateOut,
    tags=["streams"],
    summary="Get the current balance, next sequence and pending sequences",
)
def get_stream(stream_id: str) -> StreamStateOut:
    state = repo.get_stream_state(stream_id)
    if state is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "stream_not_found", "message": f"unknown stream {stream_id!r}"},
        )
    return StreamStateOut(**state.as_dict())


@app.post(
    "/streams/{stream_id}/forks",
    response_model=ForkOut,
    status_code=201,
    responses={200: {"model": ForkOut, "description": "Idempotent replay of an existing fork"}},
    tags=["forks"],
    summary="Fork the applied [0, through_sequence) prefix into a new stream",
)
def fork_stream(stream_id: str, body: ForkIn):
    try:
        fork = forks.create_fork(
            stream_id, body.fork_id, body.target_stream_id, body.through_sequence
        )
    except forks.ForkError as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail={"code": exc.code, "message": exc.message},
        )
    except sqlite3.IntegrityError as exc:
        # Defence in depth: the UNIQUE constraints (fork id, target stream)
        # map to a 409 as well.
        raise HTTPException(
            status_code=409,
            detail={"code": "integrity_conflict", "message": str(exc)},
        )
    if fork is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "stream_not_found", "message": f"unknown stream {stream_id!r}"},
        )

    payload = ForkOut(
        source_stream_id=fork.source_stream_id,
        target_stream_id=fork.target_stream_id,
        through_sequence=fork.through_sequence,
        balance=fork.balance,
        next_sequence=fork.next_sequence,
    ).model_dump()
    # Idempotent replays return 200; first-time forks return 201.
    return JSONResponse(payload, status_code=200 if fork.replayed else 201)


@app.post(
    "/streams/{stream_id}/consumers/{consumer_id}/claim",
    response_model=ClaimOut,
    status_code=200,
    tags=["consumers"],
    summary="Claim the next contiguous applied event batch (with a lease token)",
)
def claim_batch(stream_id: str, consumer_id: str, body: ClaimIn) -> ClaimOut:
    claim = consumers.claim(stream_id, consumer_id, body.max_events)
    if claim is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "stream_not_found", "message": f"unknown stream {stream_id!r}"},
        )
    return ClaimOut(**claim.as_dict())


@app.post(
    "/streams/{stream_id}/consumers/{consumer_id}/ack",
    response_model=AckOut,
    status_code=200,
    tags=["consumers"],
    summary="Acknowledge a claimed batch and atomically advance the cursor",
)
def ack_batch(stream_id: str, consumer_id: str, body: AckIn) -> AckOut:
    try:
        cursor = consumers.ack(stream_id, consumer_id, body.lease_token)
    except consumers.LeaseError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": exc.message},
        )
    if cursor is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "stream_not_found", "message": f"unknown stream {stream_id!r}"},
        )
    return AckOut(
        status="ok",
        stream_id=stream_id,
        consumer_id=consumer_id,
        cursor=cursor,
    )
