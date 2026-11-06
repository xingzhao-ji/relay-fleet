"""Actual Python-to-Java mTLS contract; no transport mocks."""
import asyncio
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import pytest
import grpc
from scripts.pki import create_pki

ROOT=Path(__file__).resolve().parents[1]

def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1',0)); return s.getsockname()[1]

class RunningFleet:
    def __init__(self, directory):
        self.path=Path(directory); self.pki=self.path/'pki'
        self.fingerprints=create_pki(self.pki)
        self.port=free_port(); self.address=f'127.0.0.1:{self.port}'
        self.binary=ROOT/'build/install/relay-fleet/bin/relay-fleet'
        self.db=self.path/'fleet.db'; self.process=None; self.logs=[]
        for tenant in ['north','south']:
            subprocess.run([str(self.binary),'bootstrap',str(self.db),self.fingerprints[f'operator-{tenant}'],tenant],check=True,capture_output=True)
    def start(self):
        log=open(self.path/f'server-{len(self.logs)}.log','w'); self.logs.append(log)
        env=os.environ.copy()
        if os.environ.get('RELAY_PROFILE')=='1':
            env['JAVA_OPTS']=env.get('JAVA_OPTS','')+f' -XX:StartFlightRecording=filename={self.path}/server-{len(self.logs)-1}.jfr,settings=profile,dumponexit=true'
        self.process=subprocess.Popen([str(self.binary),'serve',str(self.db),str(self.port),str(self.pki/'server.crt'),str(self.pki/'server.key'),str(self.pki/'ca.crt')],stdout=log,stderr=log,env=env)
    def stop(self, kill=False):
        if self.process and self.process.poll() is None:
            self.process.kill() if kill else self.process.terminate()
            self.process.wait(timeout=10)
    def channel(self, identity, address=None):
        creds=grpc.ssl_channel_credentials((self.pki/'ca.crt').read_bytes(),(self.pki/f'{identity}.key').read_bytes(),(self.pki/f'{identity}.crt').read_bytes())
        return grpc.aio.secure_channel(address or self.address,creds)
    async def ready(self):
        from agent.generated import fleet_pb2 as pb, fleet_pb2_grpc as rpc
        for _ in range(80):
            if self.process.poll() is not None: raise AssertionError('Java server exited; inspect '+str(self.path))
            try:
                async with self.channel('operator-north') as c:
                    await rpc.FleetStub(c).ListDevices(pb.ListDevicesRequest(page_size=100),timeout=.2)
                return
            except grpc.aio.AioRpcError: await asyncio.sleep(.05)
        raise AssertionError('Java server did not become ready')
    def close(self):
        self.stop()
        for f in self.logs:f.close()

@pytest.fixture
def fleet(tmp_path):
    x=RunningFleet(tmp_path);x.start()
    try:yield x
    finally:x.close()

async def sync_one(stub, frame, timeout=2):
    call=stub.Sync(timeout=timeout)
    try:
        await call.write(frame)
        return await call.read()
    finally:call.cancel()

def test_mtls_roles_tenant_isolation_replay_and_deadline(fleet):
    async def run():
        from agent.generated import fleet_pb2 as p, fleet_pb2_grpc as r
        await fleet.ready()
        async with fleet.channel('operator-north') as c, fleet.channel('operator-south') as south, fleet.channel('sim-a') as d:
            op=r.FleetStub(c); other=r.FleetStub(south); dev=r.FleetStub(d)
            await op.Register(p.RegisterRequest(device_id='sim-a',certificate_sha256=fleet.fingerprints['sim-a']),timeout=2)
            a=p.Sample(sequence=1,observed_at_ms=1000,reading_json='{"simulated":true}')
            frame=p.DeviceFrame(device_id='sim-a',schema_version=1,samples=[a])
            # Additive protobuf evolution: unknown field 100 crosses Python -> Java.
            frame.ParseFromString(frame.SerializeToString()+b'\xa0\x06\x01')
            assert (await sync_one(dev,frame)).accepted==1
            assert (await sync_one(dev,frame)).duplicates==1
            with pytest.raises(grpc.aio.AioRpcError) as err:
                await sync_one(dev,p.DeviceFrame(device_id='sim-b',schema_version=1,samples=[a]))
            assert err.value.code()==grpc.StatusCode.PERMISSION_DENIED
            with pytest.raises(grpc.aio.AioRpcError) as err:
                await other.QueryTelemetry(p.QueryRequest(device_id='sim-a',limit=10),timeout=2)
            assert err.value.code()==grpc.StatusCode.NOT_FOUND
            with pytest.raises(grpc.aio.AioRpcError) as err:
                await dev.ListDevices(p.ListDevicesRequest(page_size=100),timeout=2)
            assert err.value.code()==grpc.StatusCode.PERMISSION_DENIED
            pending=dev.Sync(timeout=.05)
            with pytest.raises(grpc.aio.AioRpcError) as err: await pending.read()
            assert err.value.code()==grpc.StatusCode.DEADLINE_EXCEEDED
            assert len((await op.QueryTelemetry(p.QueryRequest(device_id='sim-a',limit=10),timeout=2)).samples)==1
        for identity,code in [('unregistered',grpc.StatusCode.UNAUTHENTICATED),('rogue',grpc.StatusCode.UNAVAILABLE)]:
            async with fleet.channel(identity) as c:
                with pytest.raises(grpc.aio.AioRpcError) as err: await r.FleetStub(c).ListDevices(p.ListDevicesRequest(page_size=100),timeout=.7)
                assert err.value.code()==code
    asyncio.run(run())

def test_stream_limit_and_cancellation_return_admission_capacity(fleet):
    async def run():
        from agent.generated import fleet_pb2 as p, fleet_pb2_grpc as r
        await fleet.ready()
        async with fleet.channel('operator-north') as op:
            await r.FleetStub(op).Register(p.RegisterRequest(device_id='sim-a',certificate_sha256=fleet.fingerprints['sim-a']),timeout=2)
        channels=[fleet.channel('sim-a') for _ in range(3)];calls=[]
        try:
            frame=p.DeviceFrame(device_id='sim-a',schema_version=1)
            for i in range(64):
                call=r.FleetStub(channels[i//32]).Sync(timeout=10)
                calls.append(call);await call.write(frame);await call.read()
            with pytest.raises(grpc.aio.AioRpcError) as err:await sync_one(r.FleetStub(channels[2]),frame)
            assert err.value.code()==grpc.StatusCode.RESOURCE_EXHAUSTED
            for call in calls:call.cancel()
            await asyncio.sleep(.1)
            assert (await sync_one(r.FleetStub(channels[2]),frame)).ack_through==0
        finally:
            for call in calls:call.cancel()
            for c in channels:await c.close()
    asyncio.run(run())

def test_operator_cli_reports_only_certificate_tenant(fleet):
    async def run():await fleet.ready()
    asyncio.run(run())
    args=[str(ROOT/'.venv/bin/python'),'-m','agent.operator','--pki',str(fleet.pki),'--address',fleet.address]
    result=subprocess.run(args+['register','sim-a','sim-a'],cwd=ROOT,capture_output=True,text=True,check=True)
    assert json.loads(result.stdout)['tenant']=='north'
    result=subprocess.run(args+['--identity','operator-south','list'],cwd=ROOT,capture_output=True,text=True,check=True)
    assert json.loads(result.stdout)['devices']==[]
