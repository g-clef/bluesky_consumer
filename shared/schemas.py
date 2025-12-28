"""Shared data schemas for events."""
import pyarrow as pa
from typing import TypedDict, Optional


class BlueSkyEvent(TypedDict):
    """Event structure passed between services."""
    collection_timestamp: int  # Unix timestamp ms when we collected it
    event_timestamp: int  # Unix timestamp ms from the event
    did: str  # Decentralized Identifier
    event_type: str  # commit/identity/account/handle
    collection: Optional[str]  # e.g., app.bsky.feed.post
    action: Optional[str]  # create/update/delete
    cid: Optional[str]  # Content ID
    record_text: Optional[str]  # Post text if applicable
    reply_parent: Optional[str]  # Parent DID if reply
    reply_root: Optional[str]  # Root DID if reply
    embed_type: Optional[str]  # images/video/external/record
    raw_event: bytes  # Full event JSON as bytes


# Parquet schema for storage
PARQUET_SCHEMA = pa.schema([
    pa.field('collection_timestamp', pa.timestamp('ms')),
    pa.field('event_timestamp', pa.timestamp('ms')),
    pa.field('did', pa.string()),
    pa.field('event_type', pa.string()),
    pa.field('collection', pa.string()),
    pa.field('action', pa.string()),
    pa.field('cid', pa.string()),
    pa.field('record_text', pa.string()),
    pa.field('reply_parent', pa.string()),
    pa.field('reply_root', pa.string()),
    pa.field('embed_type', pa.string()),
    pa.field('raw_event', pa.binary()),
])