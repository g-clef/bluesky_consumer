"""Extract metadata from AT Protocol events."""
import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional
from atproto import CAR, models

logger = logging.getLogger(__name__)


class MetadataExtractor:
    """Extract metadata from firehose events."""

    def extract(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Extract metadata from a firehose message.

        Returns a dictionary ready for Kafka, or None if event should be skipped.
        """
        try:
            collection_timestamp = int(datetime.utcnow().timestamp() * 1000)

            # Get the operation type
            if 't' in message:
                event_type = message['t']
            else:
                event_type = 'unknown'

            # Base event structure
            event = {
                'collection_timestamp': collection_timestamp,
                'event_timestamp': collection_timestamp,  # Will update if available
                'did': None,
                'event_type': event_type,
                'collection': None,
                'action': None,
                'cid': None,
                'record_text': None,
                'reply_parent': None,
                'reply_root': None,
                'embed_type': None,
                'raw_event': json.dumps(message).encode('utf-8'),
            }

            # Handle different message types
            if event_type == '#commit':
                return self._extract_commit(message, event)
            elif event_type == '#identity':
                return self._extract_identity(message, event)
            elif event_type == '#account':
                return self._extract_account(message, event)
            elif event_type == '#handle':
                return self._extract_handle(message, event)
            else:
                logger.debug(f"Unknown event type: {event_type}")
                return event

        except Exception as e:
            logger.error(f"Error extracting metadata: {e}", exc_info=True)
            return None

    def _extract_commit(self, message: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metadata from a commit event."""
        try:
            event['did'] = message.get('did')
            event['event_timestamp'] = self._parse_time(message.get('time'))

            # Parse operations from the commit
            ops = message.get('ops', [])
            if ops:
                # Take first operation for now (could be multiple)
                op = ops[0]
                event['action'] = op.get('action')  # create/update/delete
                event['collection'] = op.get('path', '').split('/')[0] if op.get('path') else None
                event['cid'] = op.get('cid')

                # If this is a create/update with blocks, try to parse record
                if event['action'] in ['create', 'update'] and 'blocks' in message:
                    try:
                        # Decode CAR blocks to get the record
                        blocks_bytes = message['blocks']
                        if isinstance(blocks_bytes, str):
                            import base64
                            blocks_bytes = base64.b64decode(blocks_bytes)

                        # Parse record from blocks (this is simplified - real parsing is more complex)
                        # For now, just try to extract text if it's a post
                        if event['collection'] == 'app.bsky.feed.post':
                            # This would need proper CAR parsing - placeholder for now
                            event['record_text'] = self._extract_post_text(blocks_bytes)

                    except Exception as e:
                        logger.debug(f"Could not parse blocks: {e}")

            return event
        except Exception as e:
            logger.error(f"Error extracting commit: {e}")
            return event

    def _extract_identity(self, message: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metadata from an identity event."""
        event['did'] = message.get('did')
        event['event_timestamp'] = self._parse_time(message.get('time'))
        return event

    def _extract_account(self, message: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metadata from an account event."""
        event['did'] = message.get('did')
        event['event_timestamp'] = self._parse_time(message.get('time'))
        event['action'] = message.get('status')  # active/deactivated/deleted
        return event

    def _extract_handle(self, message: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metadata from a handle event."""
        event['did'] = message.get('did')
        event['event_timestamp'] = self._parse_time(message.get('time'))
        return event

    def _parse_time(self, time_str: Optional[str]) -> int:
        """Parse ISO timestamp to Unix milliseconds."""
        if not time_str:
            return int(datetime.utcnow().timestamp() * 1000)
        try:
            dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            return int(dt.timestamp() * 1000)
        except Exception:
            return int(datetime.utcnow().timestamp() * 1000)

    def _extract_post_text(self, blocks_bytes: bytes) -> Optional[str]:
        """
        Extract post text from CAR blocks.
        This is a placeholder - proper implementation would decode CAR format.
        """
        # TODO: Implement proper CAR decoding using atproto library
        # For now, return None - we'll store raw_event for later processing
        return None