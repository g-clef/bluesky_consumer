"""Extract metadata from AT Protocol events."""
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Union, List
from atproto import CAR, models, AtUri

logger = logging.getLogger(__name__)


class MetadataExtractor:
    def extract(self, message: Union[
        models.ComAtprotoSyncSubscribeRepos.Commit,
        models.ComAtprotoSyncSubscribeRepos.Identity,
        models.ComAtprotoSyncSubscribeRepos.Account,
    ]) -> Optional[List[Dict[str, Any]]]:
        """
        Extract metadata from a firehose message.

        Returns a dictionary ready for Kafka, or None if event should be skipped.
        """
        if isinstance(message, models.ComAtprotoSyncSubscribeRepos.Commit):
            return self._extract_commit(message)
        elif isinstance(message, models.ComAtprotoSyncSubscribeRepos.Identity):
            return self._extract_identity(message)
        elif isinstance(message, models.ComAtprotoSyncSubscribeRepos.Account):
            return self._extract_account(message)
        else:
            logger.debug(f"Unknown message type: {type(message)}")
            return None

    def _extract_commit(self, message: models.ComAtprotoSyncSubscribeRepos.Commit) -> Optional[List[Dict[str, Any]]]:
        """Extract metadata from a commit event."""
        try:
            blocks = CAR.from_bytes(message.blocks).blocks

            events = []
            for op in message.ops:
                try:
                    record_data = None
                    if op.cid and op.cid in blocks:
                        raw = blocks.get(op.cid)
                        record = models.get_or_create(raw, strict=False)
                        if record.py_type is not None:
                            record_data = record.model_dump()

                    uri = AtUri.from_str(f"at://{message.repo}/{op.path}")

                    event = {
                        "event_type": "commit",
                        "repo": message.repo,
                        "did": message.repo,
                        "revision": message.rev,
                        "sequence": message.seq,
                        "timestamp": message.time,
                        "event_timestamp": self._parse_time(message.time),
                        "action": op.action,
                        "path": op.path,
                        "collection": uri.collection,
                        "rkey": uri.rkey,
                    }
                    if op.cid:
                        event["cid"] = str(op.cid)
                    if record_data:
                        event["record"] = record_data
                        if uri.collection == "app.bsky.feed.post":
                            event["post_text"] = record_data.get("text")
                            event["post_langs"] = record_data.get("langs", [])
                            event["reply_parent"] = record_data.get("reply", {}).get("parent", {}).get("uri") if record_data.get("reply") else None
                            event["reply_root"] = record_data.get("reply", {}).get("root", {}).get("uri") if record_data.get("reply") else None

                            facets = record_data.get("facets", [])
                            if facets:
                                event["facets"] = facets

                            embed = record_data.get("embed", {})
                            if embed:
                                event["embed_type"] = embed.get("py_type") or embed.get("$type")
                                event["embed"] = embed

                        elif uri.collection == "app.bsky.feed.like":
                            event["like_subject"] = record_data.get("subject", {}).get("uri")

                        elif uri.collection == "app.bsky.feed.repost":
                            event["repost_subject"] = record_data.get("subject", {}).get("uri")

                        elif uri.collection == "app.bsky.graph.follow":
                            event["follow_subject"] = record_data.get("subject")

                        elif uri.collection == "app.bsky.graph.block":
                            event["block_subject"] = record_data.get("subject")

                    events.append(event)

                except Exception as e:
                    logger.error(f"Error processing op {op.path}: {e}", exc_info=True)
                    continue

            return events

        except Exception as e:
            logger.error(f"Error extracting commit: {e}", exc_info=True)
            return None

    def _extract_identity(self, message: models.ComAtprotoSyncSubscribeRepos.Identity) -> List[Dict[str, Any]]:
        """Extract metadata from an identity event."""
        return [{
            "event_type": "identity",
            "did": message.did,
            "sequence": message.seq,
            "timestamp": message.time,
            "event_timestamp": self._parse_time(message.time),
            "handle": getattr(message, 'handle', None),
        }]

    def _extract_account(self, message: models.ComAtprotoSyncSubscribeRepos.Account) -> List[Dict[str, Any]]:
        """Extract metadata from an account event."""
        return [{
            "event_type": "account",
            "did": message.did,
            "sequence": message.seq,
            "timestamp": message.time,
            "event_timestamp": self._parse_time(message.time),
            "active": message.active,
            "status": getattr(message, 'status', None),
        }]

    @staticmethod
    def _parse_time(time_str: Optional[str]) -> int:
        """Parse ISO timestamp to Unix milliseconds."""
        if not time_str:
            return int(datetime.now(timezone.utc).timestamp() * 1000)
        try:
            dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            return int(dt.timestamp() * 1000)
        except Exception:
            return int(datetime.now(timezone.utc).timestamp() * 1000)
