"""Tests unitaires des fonctions pures du producteur Kafka Vélib'."""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "kafka"))

from producer import FeedError, build_messages, parse_status_payload, serialize  # noqa: E402

STATIONS = [
    {"station_id": 1, "last_reported": 100, "num_bikes_available": 3},
    {"station_id": 2, "last_reported": 200, "num_bikes_available": 0},
]


def test_parse_status_payload_valide():
    last_updated, stations = parse_status_payload({"lastUpdatedOther": 42, "data": {"stations": STATIONS}})
    assert last_updated == 42
    assert stations == STATIONS


@pytest.mark.parametrize("payload", [[], "texte", {"data": {}}, {"data": {"stations": "pas une liste"}}])
def test_parse_status_payload_invalide(payload):
    with pytest.raises(FeedError):
        parse_status_payload(payload)


def test_build_messages_cle_station_et_enveloppe():
    messages, last_seen = build_messages(STATIONS, 42, "2026-10-07T10:00:00Z", {})
    assert [key for key, _ in messages] == ["1", "2"]
    assert messages[0][1]["station"] == STATIONS[0]
    assert messages[0][1]["source"] == "velib_gbfs_station_status"
    assert last_seen == {"1": 100, "2": 200}


def test_build_messages_capture_de_changement():
    """Une station dont last_reported n'a pas bougé n'est pas republiée."""
    _, last_seen = build_messages(STATIONS, 42, "t0", {})
    changed = [STATIONS[0], {**STATIONS[1], "last_reported": 260}]
    messages, _ = build_messages(changed, 43, "t1", last_seen)
    assert [key for key, _ in messages] == ["2"]


def test_build_messages_sans_capture_publie_tout():
    _, last_seen = build_messages(STATIONS, 42, "t0", {})
    messages, _ = build_messages(STATIONS, 43, "t1", last_seen, only_changed=False)
    assert len(messages) == 2


def test_build_messages_ignore_station_sans_id():
    messages, _ = build_messages([{"last_reported": 1}], 42, "t0", {})
    assert messages == []


def test_serialize_utf8_compact():
    raw = serialize({"nom": "Bastille - Rue de la Roquette", "n": 1})
    assert b" " not in raw.replace(b"Bastille - Rue de la Roquette", b"")
    assert json.loads(raw.decode("utf-8"))["nom"] == "Bastille - Rue de la Roquette"
