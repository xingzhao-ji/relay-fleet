"""Async network agent for simulated devices. Disk I/O stays off the event loop."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import json
import random
import time
import grpc
from .spool import Spool, QueueFull
from .generated import fleet_pb2 as pb, fleet_pb2_grpc as rpc

def emit(event, **fields):
    print(json.dumps({'time_ms':int(time.time()*1000),'event':event,**fields}),flush=True)

class AsyncSpool:
    def __init__(self,path,capacity=256):
        self.path=path;self.capacity=capacity;self.pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='spool')
        self.closed=True
    async def __aenter__(self):
        self.spool=await asyncio.get_running_loop().run_in_executor(self.pool,partial(Spool,self.path,self.capacity))
        self.closed=False
        return self
    async def call(self,method,*args,**kwargs):
        if self.closed: raise RuntimeError('spool closed')
        return await asyncio.get_running_loop().run_in_executor(self.pool,partial(getattr(self.spool,method),*args,**kwargs))
    async def __aexit__(self,*_):
        if not self.closed:
            await self.call('close');self.closed=True
        await asyncio.to_thread(self.pool.shutdown,wait=True,cancel_futures=False)

async def run_agent(*,address,pki,identity,device,spool_path,samples=120,capacity=32,interval=.02,deadline=1.0,clock_skew_ms=0,stop=None):
    stop=stop or asyncio.Event()
    credentials=grpc.ssl_channel_credentials((pki/'ca.crt').read_bytes(),(pki/f'{identity}.key').read_bytes(),(pki/f'{identity}.crt').read_bytes())
    stats={'reconnects':0,'backpressure':0,'accepted':0,'duplicates':0,'max_depth':0,'simulated':True}
    async with AsyncSpool(spool_path,capacity) as spool, grpc.aio.secure_channel(address,credentials,options=[('grpc.max_receive_message_length',80*1024)]) as channel:
        stub=rpc.FleetStub(channel)
        async def produce():
            # Generated count is durable; process restart resumes this finite workload.
            n=await spool.call('generated')
            while not stop.is_set() and n<samples:
                try:
                    n=await spool.call('append',json.dumps({'simulated':True,'temperature_c':20+(n%20)/10},separators=(',',':')),int(time.time()*1000)+clock_skew_ms)
                    depth=await spool.call('depth');stats['max_depth']=max(stats['max_depth'],depth)
                    version,body,_=await spool.call('config')
                    delay=json.loads(body)['sample_interval_ms']/1000 if version else interval
                    await asyncio.sleep(delay)
                except QueueFull:
                    stats['backpressure']+=1
                    emit('sampling_paused',device=device,reason='spool_full',depth=capacity)
                    await asyncio.sleep(.05)
        async def send():
            retry=.05
            while not stop.is_set():
                call=stub.Sync(timeout=deadline)
                try:
                    while not stop.is_set():
                        batch=await spool.call('batch',32)
                        version,_,sha=await spool.call('config')
                        sent=batch[-1]['sequence'] if batch else await spool.call('acked')
                        await call.write(pb.DeviceFrame(device_id=device,schema_version=1,samples=[pb.Sample(**s) for s in batch],applied_version=version,applied_sha256=sha))
                        reply=await call.read()
                        if reply is grpc.aio.EOF: break
                        await spool.call('ack',reply.ack_through,sent_through=sent)
                        stats['accepted']+=reply.accepted;stats['duplicates']+=reply.duplicates
                        if reply.desired.version:
                            changed=await spool.call('apply_config',reply.desired.version,reply.desired.json,reply.desired.sha256)
                            if changed:emit('config_applied',device=device,version=reply.desired.version)
                        emit('ack',device=device,through=reply.ack_through,depth=await spool.call('depth'))
                        retry=.05
                        if reply.ack_through>=samples and version==(await spool.call('config'))[0]:
                            stop.set();break
                        await asyncio.sleep(.02 if batch else .05)
                except (grpc.aio.AioRpcError,asyncio.InvalidStateError) as e:
                    # grpc.aio.write raises InvalidStateError when the transport died
                    # between frames. Read the RPC's terminal status, then classify it.
                    code=e.code() if isinstance(e,grpc.aio.AioRpcError) else await call.code()
                    stats['reconnects']+=1
                    details=e.details() if isinstance(e,grpc.aio.AioRpcError) else await call.details()
                    emit('reconnect',device=device,status=code.name,details=details,depth=await spool.call('depth'))
                    if code not in {grpc.StatusCode.UNAVAILABLE,grpc.StatusCode.DEADLINE_EXCEEDED,grpc.StatusCode.RESOURCE_EXHAUSTED,grpc.StatusCode.CANCELLED}:
                        raise
                    await asyncio.sleep(retry+random.uniform(0,retry*.2));retry=min(1,retry*2)
                finally:call.cancel()
        try:
            async with asyncio.TaskGroup() as group:
                group.create_task(produce());group.create_task(send())
        finally:
            stats.update(acked=await spool.call('acked'),remaining=await spool.call('depth'),applied_version=(await spool.call('config'))[0])
            emit('agent_summary',device=device,**stats)
    return stats
