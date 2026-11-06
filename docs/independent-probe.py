import asyncio, hashlib, json, sqlite3, tempfile, time
from pathlib import Path
import grpc
from tests.test_transport import RunningFleet, sync_one
from agent.generated import fleet_pb2 as p, fleet_pb2_grpc as r

BASE=Path.home()/'.cache/job-apps-runtime/relay-fleet/independent-review'
async def run():
    out={}
    with tempfile.TemporaryDirectory(dir=BASE) as d:
        f=RunningFleet(d);f.start()
        try:
            await f.ready()
            async with f.channel('operator-north') as oc, f.channel('sim-a') as dc:
                op=r.FleetStub(oc);dev=r.FleetStub(dc)
                await op.Register(p.RegisterRequest(device_id='sim-a',certificate_sha256=f.fingerprints['sim-a']),timeout=3)
                conf=await op.SetDesired(p.DesiredRequest(device_id='sim-a',expected_version=0,json='{"sample_interval_ms":10}'),timeout=3)
                frame=p.DeviceFrame(device_id='sim-a',schema_version=1,samples=[p.Sample(sequence=1,observed_at_ms=1,reading_json='{}')],applied_version=1,applied_sha256='bad')
                try:await sync_one(dev,frame)
                except grpc.aio.AioRpcError as e:
                    assert e.code()==grpc.StatusCode.FAILED_PRECONDITION
                else:raise AssertionError('bad applied digest accepted')
                assert len((await op.QueryTelemetry(p.QueryRequest(device_id='sim-a',limit=10),timeout=3)).samples)==0
                frame.applied_sha256=conf.desired.sha256
                assert (await sync_one(dev,frame)).accepted==1
                out['config_and_telemetry_atomicity']={'passed':True,'invalid_ack_inserted_rows':0,'valid_retry_accepted':1}
                # Valid unsigned protobuf values outside the SQL signed representation fail safely.
                frame.samples[0].sequence=2**63
                try:await sync_one(dev,frame)
                except grpc.aio.AioRpcError as e:
                    out['unsigned_sequence_overflow']=e.code().name
                    assert e.code()==grpc.StatusCode.INVALID_ARGUMENT
                else:raise AssertionError('unsigned overflow accepted')
                assert len((await op.QueryTelemetry(p.QueryRequest(device_id='sim-a',limit=10),timeout=3)).samples)==1
                # Grow a realistic maximum-size desired payload through real operator RPCs.
                prefix='{"sample_interval_ms":10,"padding":"';suffix='"}'
                body=prefix+'x'*(4096-len(prefix)-len(suffix))+suffix
                assert len(body.encode())==4096
                started=time.monotonic()
                for i in range(1001):
                    name=f'd{i:04d}'+'x'*59
                    fp=hashlib.sha256(name.encode()).hexdigest()
                    await op.Register(p.RegisterRequest(device_id=name,certificate_sha256=fp),timeout=3)
                    await op.SetDesired(p.DesiredRequest(device_id=name,expected_version=0,json=body),timeout=3)
                try:
                    result=await op.ListDevices(p.Empty(),timeout=8)
                    out['default_client_large_list']={'status':'OK','count':len(result.devices),'bytes':result.ByteSize()}
                except grpc.aio.AioRpcError as e:
                    out['default_client_large_list']={'status':e.code().name,'details':e.details()}
                cred=grpc.ssl_channel_credentials((f.pki/'ca.crt').read_bytes(),(f.pki/'operator-north.key').read_bytes(),(f.pki/'operator-north.crt').read_bytes())
                async with grpc.aio.secure_channel(f.address,cred,options=[('grpc.max_receive_message_length',8*1024*1024)]) as large:
                    result=await r.FleetStub(large).ListDevices(p.Empty(),timeout=8)
                    with sqlite3.connect(f.db) as db:count=db.execute("SELECT count(*) FROM devices WHERE tenant='north'").fetchone()[0]
                    out['large_client_list']={'registered':count,'returned':len(result.devices),'bytes':result.ByteSize(),'has_pagination_field':'next_page_token' in result.DESCRIPTOR.fields_by_name}
                out['probe_duration_s']=time.monotonic()-started
        finally:f.close()
    (BASE/'probe.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out,indent=2))
asyncio.run(run())
