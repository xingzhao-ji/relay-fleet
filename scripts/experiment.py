"""Run real process/socket failure drills and save assertions as measured evidence."""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import sqlite3
import statistics
import subprocess
import sys
import time
import uuid
import grpc
from google.protobuf.json_format import MessageToDict
from agent.generated import fleet_pb2 as pb, fleet_pb2_grpc as rpc
from agent.spool import Spool
from tests.test_transport import RunningFleet, free_port, sync_one, ROOT

class TcpCut:
    """Transparent encrypted TCP relay. Cut closes live connections and rejects new ones."""
    def __init__(self,target):
        self.target=target;self.enabled=True;self.drop_responses=False;self.writers=set();self.tasks=set()
    async def start(self):
        self.server=await asyncio.start_server(self.accept,'127.0.0.1',0)
        self.address=f'127.0.0.1:{self.server.sockets[0].getsockname()[1]}'
    async def accept(self,reader,writer):
        task=asyncio.current_task();self.tasks.add(task)
        remote=None
        try:
            if not self.enabled:return
            upstream,remote=await asyncio.open_connection('127.0.0.1',self.target)
            self.writers.update([writer,remote])
            async def pump(source,dest,downstream=False):
                while data:=await source.read(65536):
                    if downstream and self.drop_responses:continue
                    dest.write(data);await dest.drain()
            one=asyncio.create_task(pump(reader,remote));two=asyncio.create_task(pump(upstream,writer,True))
            try:
                _,pending=await asyncio.wait([one,two],return_when=asyncio.FIRST_COMPLETED)
                for t in pending:t.cancel()
                await asyncio.gather(one,two,return_exceptions=True)
            finally:
                one.cancel();two.cancel()
        except (ConnectionError,OSError):pass
        finally:
            writer.close();self.writers.discard(writer)
            if remote:remote.close();self.writers.discard(remote)
            self.tasks.discard(task)
    def cut(self):
        self.enabled=False
        for w in list(self.writers):w.close()
    async def close(self):
        self.cut();self.server.close();await self.server.wait_closed()
        for t in list(self.tasks):t.cancel()
        await asyncio.gather(*self.tasks,return_exceptions=True)

def query_db(path,sql,params=()):
    with sqlite3.connect(path) as db:return db.execute(sql,params).fetchall()

async def until(check,timeout=12):
    start=time.monotonic()
    while not check():
        assert time.monotonic()-start<timeout,'timed out awaiting measured state'
        await asyncio.sleep(.03)

async def drill(directory,out):
    directory.mkdir(parents=True,exist_ok=True)
    fleet=RunningFleet(directory);fleet.start()
    proxy=TcpCut(fleet.port);await proxy.start()
    children=[];logs=[]
    report={'schema_version':1,'kind':'local_simulated_fleet_failure_experiment','simulation':True,
            'hardware_claim':False,'started_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),
            'environment':{'os':platform.platform(),'python':platform.python_version(),'machine':platform.machine(),'java':'21'},
            'timeline':[],'checks':{},'method':{'devices':3,'samples_per_device':160,'offline_capacity':16,'batch_max':32,'rpc_deadline_s':.7,'network':'loopback TCP relay forcibly closes live sockets; TLS remains end to end'}}
    begin=time.monotonic()
    def event(name,**data):report['timeline'].append({'event':name,'elapsed_s':round(time.monotonic()-begin,4),**data})
    def agent(identity,device):
        log=open(directory/f'{identity}-{len(children)}.log','w');logs.append(log)
        env=os.environ.copy()
        if os.environ.get('RELAY_GRPC_TRACE')=='1':env.update(GRPC_VERBOSITY='DEBUG',GRPC_TRACE='handshaker,tsi')
        p=subprocess.Popen([sys.executable,'-m','agent','--address',proxy.address,'--pki',str(fleet.pki),'--identity',identity,
                            '--device',device,'--spool',str(directory/f'{identity}.db'),'--samples','160','--capacity','16',
                            '--interval','.01','--deadline','.7','--clock-skew-ms','3600000'],cwd=ROOT,stdout=log,stderr=log,env=env)
        children.append(p);return p
    try:
        await fleet.ready()
        async with fleet.channel('operator-north') as north, fleet.channel('operator-south') as south:
            op=rpc.FleetStub(north);other=rpc.FleetStub(south)
            for stub,identity,device in [(op,'sim-a','sim-a'),(op,'sim-b','sim-b'),(other,'south-a','sim-a')]:
                await stub.Register(pb.RegisterRequest(device_id=device,certificate_sha256=fleet.fingerprints[identity]),timeout=2)
                await stub.SetDesired(pb.DesiredRequest(device_id=device,expected_version=0,json='{"sample_interval_ms":10}'),timeout=2)
            denied={}
            async with fleet.channel('sim-a') as d:
                device_stub=rpc.FleetStub(d)
                for name,operation,expected in [
                    ('device_spoof',lambda:sync_one(device_stub,pb.DeviceFrame(device_id='sim-b',schema_version=1)),grpc.StatusCode.PERMISSION_DENIED),
                    ('device_operator_rpc',lambda:device_stub.ListDevices(pb.ListDevicesRequest(page_size=100),timeout=2),grpc.StatusCode.PERMISSION_DENIED),
                    ('cross_tenant_query',lambda:other.QueryTelemetry(pb.QueryRequest(device_id='sim-b',limit=1),timeout=2),grpc.StatusCode.NOT_FOUND),
                ]:
                    try:await operation();raise AssertionError(f'{name} accepted')
                    except grpc.aio.AioRpcError as e:
                        assert e.code()==expected;denied[name]=e.code().name
            for identity,expected in [('unregistered',grpc.StatusCode.UNAUTHENTICATED),('rogue',grpc.StatusCode.UNAVAILABLE)]:
                async with fleet.channel(identity) as c:
                    try:await rpc.FleetStub(c).ListDevices(pb.ListDevicesRequest(page_size=100),timeout=.5);raise AssertionError(f'{identity} accepted')
                    except grpc.aio.AioRpcError as e:
                        assert e.code()==expected;denied[identity]=e.code().name
            report['checks']['negative_authorization']={'passed':True,'statuses':denied}
            agents=[agent('sim-a','sim-a'),agent('sim-b','sim-b'),agent('south-a','sim-a')]
            await until(lambda:query_db(fleet.db,'SELECT min(last_seq) FROM devices')[0][0]>=8)
            pre_cut=query_db(fleet.db,'SELECT tenant,device,last_seq FROM devices ORDER BY tenant,device')
            event('connected',committed=pre_cut)
            proxy.cut();event('network_cut_live_sockets')
            await until(lambda:all(query_db(directory/f'{n}.db','SELECT count(*) FROM spool')[0][0]==16 for n in ['sim-a','sim-b','south-a']))
            depths={n:query_db(directory/f'{n}.db','SELECT count(*) FROM spool')[0][0] for n in ['sim-a','sim-b','south-a']}
            event('offline_queues_full',depths=depths)
            before=query_db(directory/'sim-a.db','SELECT sequence FROM spool ORDER BY sequence')
            agents[0].kill();await asyncio.to_thread(agents[0].wait,5)
            fleet.stop(kill=True);event('sigkill_agent_and_control_plane')
            fleet.start();await fleet.ready()
            after=query_db(directory/'sim-a.db','SELECT sequence FROM spool ORDER BY sequence')
            assert before==after
            report['checks']['spool_survived_sigkill']={'passed':True,'pending_sequences':[x[0] for x in after]}
            agents[0]=agent('sim-a','sim-a')
            # New desired state is delivered after reconnection, and each agent reports it durably.
            for stub,device in [(op,'sim-a'),(op,'sim-b'),(other,'sim-a')]:
                await stub.SetDesired(pb.DesiredRequest(device_id=device,expected_version=1,json='{"sample_interval_ms":10,"revision":"after-outage"}'),timeout=3)
            proxy.enabled=True;event('network_restored')
            await until(lambda:all(p.poll() is not None for p in agents),timeout=20)
            for p in agents:assert p.returncode==0,f'agent failed: {p.returncode}'
            rows=query_db(fleet.db,'SELECT tenant,device,count(*),min(seq),max(seq),count(DISTINCT seq) FROM telemetry GROUP BY tenant,device ORDER BY tenant,device')
            assert len(rows)==3 and all(r[2:]==(160,1,160,160) for r in rows)
            assert all(query_db(directory/f'{n}.db','SELECT count(*) FROM spool')[0][0]==0 for n in ['sim-a','sim-b','south-a'])
            report['checks']['recovery_no_missing_or_duplicate_effects']={'passed':True,'rows':rows,'total':480}
            devices=(await op.ListDevices(pb.ListDevicesRequest(page_size=100),timeout=2)).devices
            assert len(devices)==2 and all(d.applied_version==2 and d.desired.version==2 for d in devices)
            assert all(d.clock_skew_ms>3_500_000 for d in devices)
            report['checks']['config_converged_after_restart_and_clock_skew']={'passed':True,'desired_applied_version':2,'clock_skew_ms':[d.clock_skew_ms for d in devices]}
            report['checks']['tenant_isolation_same_device_name']={'passed':True,'north_devices':len(devices),'south_devices':len((await other.ListDevices(pb.ListDevicesRequest(page_size=100),timeout=2)).devices)}
            event('fleet_converged',total_telemetry=480)
            # ACK-loss drill: actual encrypted response bytes are discarded AFTER handshake.
            await op.Register(pb.RegisterRequest(device_id='replay',certificate_sha256=fleet.fingerprints['replay']),timeout=2)
            async with fleet.channel('replay',proxy.address) as c:
                stub=rpc.FleetStub(c)
                await sync_one(stub,pb.DeviceFrame(device_id='replay',schema_version=1))
                with Spool(directory/'replay.db',8) as spool:
                    spool.append('{"simulated":true,"v":7}',1000)
                    frame=pb.DeviceFrame(device_id='replay',schema_version=1,samples=[pb.Sample(**x) for x in spool.batch()])
                    proxy.drop_responses=True
                    call=stub.Sync(timeout=.25);await call.write(frame)
                    await until(lambda:query_db(fleet.db,"SELECT count(*) FROM telemetry WHERE device='replay'")[0][0]==1)
                    try:await call.read();raise AssertionError('ACK unexpectedly received')
                    except grpc.aio.AioRpcError as e:assert e.code()==grpc.StatusCode.DEADLINE_EXCEEDED
                    finally:call.cancel()
                    assert spool.depth()==1
                    # Discarding ciphertext breaks TLS record continuity: reconnect
                    # instead of reusing the deliberately truncated TLS session.
                    proxy.cut();proxy.drop_responses=False;proxy.enabled=True
                    async with fleet.channel('replay',proxy.address) as recovered:
                        response=await sync_one(rpc.FleetStub(recovered),frame)
                    assert response.duplicates==1 and response.accepted==0
                    spool.ack(response.ack_through,sent_through=1)
                    assert spool.depth()==0
                    report['checks']['lost_ack_replay_exactly_one_effect']={'passed':True,'duplicate_ack':response.duplicates,'database_rows':1,'spool_rows_after_ack':0}
                conflict=pb.DeviceFrame(device_id='replay',schema_version=1,samples=[pb.Sample(sequence=1,observed_at_ms=1000,reading_json='changed')])
                async with fleet.channel('replay',proxy.address) as recovered:
                    try:await sync_one(rpc.FleetStub(recovered),conflict);raise AssertionError('conflict accepted')
                    except grpc.aio.AioRpcError as e:assert e.code()==grpc.StatusCode.ALREADY_EXISTS
                report['checks']['conflicting_sequence_rejected']={'passed':True,'status':'ALREADY_EXISTS'}
            # Stale CAS and stale config ACK cannot regress current state.
            try:await op.SetDesired(pb.DesiredRequest(device_id='sim-a',expected_version=1,json='{"sample_interval_ms":100}'),timeout=2);raise AssertionError('stale update accepted')
            except grpc.aio.AioRpcError as e:assert e.code()==grpc.StatusCode.ABORTED
            async with fleet.channel('sim-a') as c:
                stale=pb.DeviceFrame(device_id='sim-a',schema_version=1,applied_version=1,applied_sha256=hashlib.sha256(b'{"sample_interval_ms":10}').hexdigest())
                await sync_one(rpc.FleetStub(c),stale)
            assert next(d for d in (await op.ListDevices(pb.ListDevicesRequest(page_size=100),timeout=2)).devices if d.device_id=='sim-a').applied_version==2
            report['checks']['stale_config_cannot_overwrite']={'passed':True,'desired_cas_status':'ABORTED','applied_after_stale_ack':2}
            # Revocation also applies to an already established stream.
            async with fleet.channel('replay') as c:
                call=rpc.FleetStub(c).Sync(timeout=2)
                await call.write(pb.DeviceFrame(device_id='replay',schema_version=1));await call.read()
                await op.Revoke(pb.DeviceId(device_id='replay'),timeout=2)
                await call.write(pb.DeviceFrame(device_id='replay',schema_version=1))
                try:await call.read();raise AssertionError('revoked stream accepted')
                except grpc.aio.AioRpcError as e:assert e.code()==grpc.StatusCode.PERMISSION_DENIED
                finally:call.cancel()
            report['checks']['revocation_on_existing_stream']={'passed':True,'status':'PERMISSION_DENIED'}
            # Warm local throughput measurement: fixed batches, FULL durability, real TLS.
            await op.Register(pb.RegisterRequest(device_id='bench',certificate_sha256=fleet.fingerprints['bench']),timeout=2)
            async with fleet.channel('bench') as c:
                stub=rpc.FleetStub(c);call=stub.Sync(timeout=10);latencies=[];total=0
                warm=pb.DeviceFrame(device_id='bench',schema_version=1)
                await call.write(warm);await call.read()
                t=time.perf_counter()
                for batch in range(100):
                    frame=pb.DeviceFrame(device_id='bench',schema_version=1,samples=[pb.Sample(sequence=batch*32+i+1,observed_at_ms=1000,reading_json='{"simulated":true,"temperature_c":21}') for i in range(32)])
                    one=time.perf_counter();await call.write(frame);ack=await call.read();latencies.append((time.perf_counter()-one)*1000);total+=ack.accepted
                elapsed=time.perf_counter()-t;call.cancel()
                assert total==3200
                latencies.sort()
                report['performance']={'method':'100 sequential 32-sample batches on one warmed mTLS bidi stream; SQLite WAL synchronous FULL; local loopback; no disk fsync interception; includes commit + ACK, excludes PKI/startup/warmup','samples':total,'batches':100,'elapsed_s':elapsed,'samples_per_s':total/elapsed,'batch_p50_ms':statistics.median(latencies),'batch_p95_ms':latencies[94],'batch_p99_ms':latencies[98],'batch_latency_ms':latencies,'not_a_scale_claim':True}
            report['operator_snapshot']=MessageToDict(await op.ListDevices(pb.ListDevicesRequest(page_size=100),timeout=2),preserving_proto_field_name=True,always_print_fields_with_no_presence=True)
            event('checks_complete')
    finally:
        for p in children:
            if p.poll() is None:p.terminate();await asyncio.to_thread(p.wait,5)
        for f in logs:f.close()
        fleet.close();await proxy.close()
    report['passed']=True;report['duration_s']=round(time.monotonic()-begin,3)
    out.mkdir(parents=True,exist_ok=True)
    (out/'failure-experiment.json').write_text(json.dumps(report,indent=2)+'\n')
    for log in directory.glob('*.log'):shutil.copy(log,out/log.name)
    print(json.dumps({'passed':True,'checks':len(report['checks']),'telemetry_recovered':480,'performance':{k:v for k,v in report['performance'].items() if k!='batch_latency_ms'}}))
    return report

def main():
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,default=ROOT/'evidence');p.add_argument('--runtime',type=Path)
    args=p.parse_args();directory=args.runtime or ROOT/'.runtime'/f'run-{uuid.uuid4().hex[:8]}'
    asyncio.run(drill(directory,args.output))

if __name__=='__main__':main()
