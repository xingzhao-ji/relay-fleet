"""Catches data loss, sequence reuse, ACK overreach and stale local config."""
import hashlib
import pytest
from agent.spool import Spool, QueueFull

def test_bounded_spool_survives_restart_without_reusing_sequence(tmp_path):
    p = tmp_path / 'spool.db'
    s = Spool(p, capacity=2)
    assert s.append('{}', 1000) == 1
    assert s.append('{"v":2}', 999) == 2
    with pytest.raises(QueueFull): s.append('{}', 1001)
    s.close()
    s = Spool(p, capacity=2)
    assert [r['sequence'] for r in s.batch()] == [1, 2]
    s.ack(1, sent_through=2)
    assert s.append('{}', 1002) == 3
    s.close()

def test_ack_cannot_delete_unsent_or_unallocated_rows(tmp_path):
    with Spool(tmp_path/'s.db', capacity=8) as s:
        for _ in range(3): s.append('{}', 1000)
        with pytest.raises(ValueError): s.ack(3, sent_through=2)
        assert len(s.batch()) == 3
        s.ack(2, sent_through=2)
        s.ack(1, sent_through=2)
        assert [r['sequence'] for r in s.batch()] == [3]

def test_config_is_durable_monotonic_and_digest_checked(tmp_path):
    body = '{"sample_interval_ms":100}'
    sha = hashlib.sha256(body.encode()).hexdigest()
    p = tmp_path/'s.db'
    with Spool(p, capacity=8) as s:
        assert s.apply_config(2, body, sha)
        assert not s.apply_config(1, '{}', hashlib.sha256(b'{}').hexdigest())
        with pytest.raises(ValueError): s.apply_config(2, '{}', hashlib.sha256(b'{}').hexdigest())
        with pytest.raises(ValueError): s.apply_config(3, body, 'bad')
    with Spool(p, capacity=8) as s:
        assert s.config() == (2, body, sha)

def test_payload_bound_does_not_allocate_sequence(tmp_path):
    with Spool(tmp_path/'s.db',capacity=2) as s:
        with pytest.raises(ValueError): s.append('x'*2049, 1)
        assert s.append('{}', 1) == 1
