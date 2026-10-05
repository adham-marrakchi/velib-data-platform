"""Tests for Kafka producer event generation."""

import json
import os
import sys
import uuid

# Add kafka directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "kafka"))


def test_generate_event():
    """Test that generate_event produces valid sensor events."""
    from producer import generate_event

    event = generate_event()

    assert "event_id" in event
    assert "timestamp" in event
    assert "device_id" in event
    assert "reading_value" in event

    # Validate types
    assert isinstance(event["event_id"], str)
    assert isinstance(event["timestamp"], int)
    assert isinstance(event["device_id"], int)
    assert isinstance(event["reading_value"], float)

    # Validate ranges
    assert 1000 <= event["device_id"] <= 9999
    assert 20.0 <= event["reading_value"] <= 80.0

    # Validate UUID format
    uuid.UUID(event["event_id"])

    # Validate JSON serializable
    json.dumps(event)


def test_generate_event_uniqueness():
    """Test that events have unique IDs."""
    from producer import generate_event

    events = [generate_event() for _ in range(100)]
    event_ids = [e["event_id"] for e in events]
    assert len(set(event_ids)) == 100
