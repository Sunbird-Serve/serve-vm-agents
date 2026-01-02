"""
SQLAlchemy Core table definitions for serve_agent_sessions and serve_agent_events
"""
from sqlalchemy import (
    Table, Column, MetaData,
    UUID, String, Text, Boolean, DateTime, JSON,
    ForeignKey, Index
)
from sqlalchemy.dialects.postgresql import JSONB
import uuid

metadata = MetaData()

# serve_agent_sessions table
serve_agent_sessions = Table(
    "serve_agent_sessions",
    metadata,
    Column("session_id", UUID, primary_key=True, default=uuid.uuid4),
    Column("wa_phone", Text, unique=True, nullable=False),
    Column("conversation_id", Text, nullable=True),
    Column("state", Text, nullable=False),
    Column("sub_state", Text, nullable=True),
    Column("last_agent_prompt_id", Text, nullable=True),
    Column("last_outbound_msg_id", Text, nullable=True),
    Column("temp_name", Text, nullable=True),
    Column("temp_email", Text, nullable=True),
    Column("temp_phone", Text, nullable=True),
    Column("eligibility_status", Text, nullable=True),
    Column("eligibility_fail_reason", Text, nullable=True),
    Column("eligibility_checked_at", DateTime(timezone=True), nullable=True),
    Column("device_policy", Text, nullable=False, server_default="laptop_or_tablet_only"),
    Column("available_days", JSONB, nullable=True),
    Column("available_time_bands", JSONB, nullable=True),
    Column("retries", JSONB, nullable=True),
    Column("tool_state", JSONB, nullable=True),
    Column("ended", Boolean, nullable=False, server_default="false"),
    Column("end_reason", Text, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default="NOW()"),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default="NOW()"),
    Column("expires_at", DateTime(timezone=True), nullable=False),
    Index("idx_sessions_state", "state"),
    Index("idx_sessions_updated_at", "updated_at"),
    Index("idx_sessions_expires_at", "expires_at"),
)

# serve_agent_events table
serve_agent_events = Table(
    "serve_agent_events",
    metadata,
    Column("event_id", UUID, primary_key=True, default=uuid.uuid4),
    Column("session_id", UUID, ForeignKey("serve_agent_sessions.session_id"), nullable=False),
    Column("wa_phone", Text, nullable=False),
    Column("agent_name", Text, nullable=False),
    Column("state", Text, nullable=True),
    Column("sub_state", Text, nullable=True),
    Column("event_type", Text, nullable=False),
    Column("event_source", Text, nullable=False),
    Column("event_status", Text, nullable=True),
    Column("details", JSONB, nullable=True),
    Column("occurred_at", DateTime(timezone=True), nullable=False, server_default="NOW()"),
)

