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
                # Independent recheck of the original workload with the corrected API.
                all_ids=[];token='';sizes=[];pages=[]
                while True:
                    result=await op.ListDevices(p.ListDevicesRequest(page_size=73,page_token=token),timeout=8)
                    all_ids.extend(x.device_id for x in result.devices)
                    pages.append(len(result.devices));sizes.append(result.ByteSize())
                    token=result.next_page_token
                    if not token:break
                expected=[f'd{i:04d}'+'x'*59 for i in range(1001)]+['sim-a']
                assert all_ids==expected
                assert len(set(all_ids))==1002
                assert max(sizes)<512*1024
                out['default_client_large_list']={'status':'OK','registered':1002,'returned':len(all_ids),'unique':len(set(all_ids)),'page_size':73,'page_counts':pages,'maximum_response_bytes':max(sizes),'default_4mib_limit_unchanged':True}
                legacy=oc.unary_unary('/relay.v1.Fleet/ListDevices',request_serializer=p.Empty.SerializeToString,response_deserializer=p.Devices.FromString)
                try:await legacy(p.Empty(),timeout=3)
                except grpc.aio.AioRpcError as e:
                    assert e.code()==grpc.StatusCode.INVALID_ARGUMENT
                    out['legacy_request']=e.code().name
                else:raise AssertionError('legacy request silently accepted')
                out['probe_duration_s']=time.monotonic()-started
        finally:f.close()
    (BASE/'pagination-recheck.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out,indent=2))
asyncio.run(run())
