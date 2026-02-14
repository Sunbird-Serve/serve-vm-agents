"""
Reminder persistence and processing (stored in serve_agent_sessions.tool_state JSONB).

Design goals:
- No dedicated DB table (per product request)
- Best-effort: never block onboarding flow
- Minimal locking to avoid duplicate sends across concurrent workers
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import select, update

from .tables import serve_agent_sessions

log = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(dt: str) -> Optional[datetime]:
    if not dt:
        return None
    try:
        # fromisoformat doesn't accept Z; normalize if needed
        return datetime.fromisoformat(dt.replace("Z", "+00:00"))
    except Exception:
        return None


def _load_tool_state(db: Session, wa_phone: str) -> Dict[str, Any]:
    stmt = select(serve_agent_sessions.c.tool_state).where(serve_agent_sessions.c.wa_phone == wa_phone)
    row = db.execute(stmt).first()
    if row and row[0] and isinstance(row[0], dict):
        return row[0].copy()
    return {}


def add_reminder(
    db: Session,
    wa_phone: str,
    *,
    when_iso: str,
    reason: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Append a reminder into tool_state.reminders.

    Reminder shape:
      {
        "id": "<uuid>",
        "when_iso": "<iso>",
        "reason": "...",
        "status": "scheduled",
        "created_at": "<iso>",
        "attempts": 0,
        "payload": {...}   # arbitrary (e.g., message text/buttons/template)
      }
    """
    tool_state = _load_tool_state(db, wa_phone)
    reminders: List[Dict[str, Any]] = []
    if isinstance(tool_state.get("reminders"), list):
        reminders = list(tool_state["reminders"])

    # Dedupe: if a similar reminder is already scheduled around the same time, skip creating another.
    target_dt = _parse_iso(when_iso)
    if target_dt:
        for existing in reminders:
            if not isinstance(existing, dict):
                continue
            if existing.get("reason") != reason:
                continue
            if existing.get("status") not in {"scheduled", "sending"}:
                continue
            existing_dt = _parse_iso(existing.get("when_iso"))
            if existing_dt and abs((existing_dt - target_dt).total_seconds()) <= 60 * 60:
                return existing

    reminder = {
        "id": str(uuid.uuid4()),
        "when_iso": when_iso,
        "reason": reason,
        "status": "scheduled",
        "created_at": _now_iso(),
        "attempts": 0,
        "last_attempt_at": None,
        "sent_at": None,
        "last_error": None,
        "payload": payload or {},
    }
    reminders.append(reminder)
    tool_state["reminders"] = reminders

    db.execute(
        update(serve_agent_sessions)
        .where(serve_agent_sessions.c.wa_phone == wa_phone)
        .values(tool_state=tool_state, updated_at=datetime.now(timezone.utc))
    )
    return reminder


def list_due_reminders(
    db: Session,
    *,
    now: datetime,
    limit_sessions: int = 200,
) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Return [(wa_phone, reminder_dict)] for due reminders.
    Filtering is done in Python to avoid complex JSONB querying without migrations.
    """
    stmt = (
        select(serve_agent_sessions.c.wa_phone, serve_agent_sessions.c.tool_state, serve_agent_sessions.c.ended)
        .where(serve_agent_sessions.c.tool_state.is_not(None))
        .order_by(serve_agent_sessions.c.updated_at.desc())
        .limit(limit_sessions)
    )
    out: List[Tuple[str, Dict[str, Any]]] = []
    for wa_phone, tool_state, ended in db.execute(stmt).all():
        if ended:
            continue
        if not tool_state or not isinstance(tool_state, dict):
            continue
        reminders = tool_state.get("reminders")
        if not isinstance(reminders, list):
            continue
        for r in reminders:
            if not isinstance(r, dict):
                continue
            if r.get("status") != "scheduled":
                continue
            when_dt = _parse_iso(r.get("when_iso"))
            if when_dt and when_dt <= now:
                out.append((wa_phone, r))
    return out


def lock_reminder(
    db: Session,
    wa_phone: str,
    reminder_id: str,
    *,
    worker_id: str,
) -> bool:
    """
    Best-effort lock: mark reminder status=sending with locked_by/locked_at.
    Returns True if lock applied, False if reminder not found or already not schedulable.
    """
    tool_state = _load_tool_state(db, wa_phone)
    reminders = tool_state.get("reminders")
    if not isinstance(reminders, list):
        return False

    changed = False
    for r in reminders:
        if isinstance(r, dict) and r.get("id") == reminder_id:
            if r.get("status") != "scheduled":
                return False
            r["status"] = "sending"
            r["locked_by"] = worker_id
            r["locked_at"] = _now_iso()
            r["attempts"] = int(r.get("attempts") or 0) + 1
            r["last_attempt_at"] = _now_iso()
            changed = True
            break
    if not changed:
        return False

    db.execute(
        update(serve_agent_sessions)
        .where(serve_agent_sessions.c.wa_phone == wa_phone)
        .values(tool_state=tool_state, updated_at=datetime.now(timezone.utc))
    )
    return True


def mark_reminder_sent(db: Session, wa_phone: str, reminder_id: str) -> None:
    tool_state = _load_tool_state(db, wa_phone)
    reminders = tool_state.get("reminders")
    if not isinstance(reminders, list):
        return
    for r in reminders:
        if isinstance(r, dict) and r.get("id") == reminder_id:
            r["status"] = "sent"
            r["sent_at"] = _now_iso()
            r["last_error"] = None
            break
    db.execute(
        update(serve_agent_sessions)
        .where(serve_agent_sessions.c.wa_phone == wa_phone)
        .values(tool_state=tool_state, updated_at=datetime.now(timezone.utc))
    )


def mark_reminder_failed(
    db: Session,
    wa_phone: str,
    reminder_id: str,
    *,
    error: str,
    retry_after_minutes: int = 60,
    max_attempts: int = 3,
) -> None:
    tool_state = _load_tool_state(db, wa_phone)
    reminders = tool_state.get("reminders")
    if not isinstance(reminders, list):
        return
    now = datetime.now(timezone.utc)
    for r in reminders:
        if isinstance(r, dict) and r.get("id") == reminder_id:
            attempts = int(r.get("attempts") or 0)
            r["last_error"] = (error or "")[:500]
            if attempts >= max_attempts:
                r["status"] = "failed"
            else:
                r["status"] = "scheduled"
                r["when_iso"] = (now + timedelta(minutes=retry_after_minutes)).isoformat()
            break
    db.execute(
        update(serve_agent_sessions)
        .where(serve_agent_sessions.c.wa_phone == wa_phone)
        .values(tool_state=tool_state, updated_at=now)
    )


