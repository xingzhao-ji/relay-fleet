import asyncio
import pytest
from agent.runtime import AsyncSpool
from agent.runtime import run_agent
from agent.spool import Spool
from scripts.pki import create_pki
import threading

def test_async_spool_serializes_concurrent_producers_and_closes(tmp_path):
    async def run():
        async with AsyncSpool(tmp_path/'s.db',capacity=64) as s:
            results=await asyncio.gather(*(s.call('append','{}',i) for i in range(32)))
            assert sorted(results)==list(range(1,33))
            assert await s.call('depth')==32
        with pytest.raises(RuntimeError): await s.call('depth')
    asyncio.run(run())

def test_cancellation_closes_network_and_disk_worker_with_data_intact(tmp_path):
    pki=tmp_path/'pki';create_pki(pki)
    async def run():
        task=asyncio.create_task(run_agent(address='127.0.0.1:1',pki=pki,identity='sim-a',device='sim-a',spool_path=tmp_path/'s.db',samples=100,capacity=8,interval=.01,deadline=.1))
        await asyncio.sleep(.15)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):await task
        assert not any(t.name.startswith('spool') for t in threading.enumerate())
    asyncio.run(run())
    with Spool(tmp_path/'s.db',capacity=8) as s:
        assert 1<=s.depth()<=8
        assert s.acked()==0
