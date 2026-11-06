"""Durable simulation input and applied-config journal; one owner per DB file."""
import hashlib
import json
import sqlite3


class QueueFull(Exception):
    """Producer must pause: an unacknowledged sample is never evicted."""


class Spool:
    def __init__(self, path, capacity=256):
        if not 1 <= capacity <= 100_000:
            raise ValueError('capacity must be 1..100000')
        self.capacity = capacity
        self.db = sqlite3.connect(path, timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS state (
                id INTEGER PRIMARY KEY CHECK(id=1), next_seq INTEGER NOT NULL,
                acked INTEGER NOT NULL, config_version INTEGER NOT NULL,
                config_json TEXT NOT NULL, config_sha TEXT NOT NULL);
            INSERT OR IGNORE INTO state VALUES(1,1,0,0,'','');
            CREATE TABLE IF NOT EXISTS spool (
                sequence INTEGER PRIMARY KEY, observed_at_ms INTEGER NOT NULL,
                reading_json TEXT NOT NULL);
        ''')

    def append(self, reading_json, observed_at_ms):
        if len(reading_json.encode()) > 2048:
            raise ValueError('sample exceeds 2048 bytes')
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            if self.depth() >= self.capacity:
                raise QueueFull('journal full; sampling paused')
            seq = self.db.execute('SELECT next_seq FROM state').fetchone()[0]
            self.db.execute('INSERT INTO spool VALUES(?,?,?)', (seq, observed_at_ms, reading_json))
            self.db.execute('UPDATE state SET next_seq=next_seq+1 WHERE id=1')
        return seq

    def depth(self):
        return self.db.execute('SELECT count(*) FROM spool').fetchone()[0]

    def batch(self, size=32):
        if not 1 <= size <= 32:
            raise ValueError('batch size must be 1..32')
        return [dict(r) for r in self.db.execute('SELECT * FROM spool ORDER BY sequence LIMIT ?', (size,))]

    def ack(self, through, *, sent_through):
        state = self.db.execute('SELECT next_seq,acked FROM state').fetchone()
        if through < 0 or through >= state['next_seq'] or through > sent_through:
            raise ValueError('ACK exceeds allocated/sent sequence; refusing deletion')
        with self.db:
            self.db.execute('DELETE FROM spool WHERE sequence <= ?', (through,))
            self.db.execute('UPDATE state SET acked=max(acked,?) WHERE id=1', (through,))

    def acked(self):
        return self.db.execute('SELECT acked FROM state').fetchone()[0]

    def generated(self):
        return self.db.execute('SELECT next_seq-1 FROM state').fetchone()[0]

    def config(self):
        r = self.db.execute('SELECT config_version,config_json,config_sha FROM state').fetchone()
        return tuple(r)

    def apply_config(self, version, body, sha):
        current, old_body, old_sha = self.config()
        if version < current:
            return False
        if version == current:
            if (body, sha) != (old_body, old_sha):
                raise ValueError('same config version has conflicting content')
            return False
        if len(body.encode()) > 4096 or hashlib.sha256(body.encode()).hexdigest() != sha:
            raise ValueError('config digest/size invalid')
        value = json.loads(body)
        interval = value.get('sample_interval_ms') if isinstance(value, dict) else None
        if type(interval) is not int or not 10 <= interval <= 60_000:
            raise ValueError('sample_interval_ms must be integer 10..60000')
        # Applying config changes the simulation's durable state. No firmware/physical side effect.
        with self.db:
            self.db.execute('UPDATE state SET config_version=?,config_json=?,config_sha=? WHERE id=1',
                            (version, body, sha))
        return True

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
