"""Regression for the independently reproduced oversized/truncated fleet list."""
import asyncio
import base64
import hashlib
import json
import subprocess
import grpc
import pytest
from agent.generated import fleet_pb2 as pb, fleet_pb2_grpc as rpc
from tests.test_transport import ROOT, RunningFleet


def request(size=100, token=''):
    return pb.ListDevicesRequest(page_size=size, page_token=token)


def test_maximum_config_fleet_is_complete_bounded_and_tenant_scoped(tmp_path):
    fleet=RunningFleet(tmp_path)
    tenant='n'*64
    subprocess.run([str(fleet.binary),'bootstrap',str(fleet.db),fleet.fingerprints['unregistered'],tenant],check=True,capture_output=True)
    fleet.start()
    ids=[f'd{i:04d}'+'x'*59 for i in range(1025)]
    prefix='{"sample_interval_ms":10,"padding":"'
    body=prefix+'x'*(4096-len(prefix)-2)+'"}'
    assert len(body.encode())==4096 and all(len(name)==64 for name in ids)
    observations={}

    async def run():
        await fleet.ready()
        async with fleet.channel('unregistered') as c, fleet.channel('operator-north') as other:
            stub=rpc.FleetStub(c);north=rpc.FleetStub(other)
            await north.Register(pb.RegisterRequest(device_id=ids[0],certificate_sha256=fleet.fingerprints['sim-a']),timeout=3)
            for name in ids:
                await stub.Register(pb.RegisterRequest(device_id=name,certificate_sha256=hashlib.sha256((tenant+name).encode()).hexdigest()),timeout=3)
                await stub.SetDesired(pb.DesiredRequest(device_id=name,expected_version=0,json=body),timeout=3)
            seen=[];token='';page_sizes=[];response_bytes=[];first_token=None
            while True:
                page=await stub.ListDevices(request(100,token),timeout=3)
                assert len(page.devices)<=100
                assert page.ByteSize()<=512*1024
                assert all(d.tenant==tenant and len(d.desired.json.encode())==4096 for d in page.devices)
                seen.extend(d.device_id for d in page.devices)
                page_sizes.append(len(page.devices));response_bytes.append(page.ByteSize())
                token=page.next_page_token
                if first_token is None:first_token=token
                if not token:break
            assert seen==ids and len(set(seen))==1025
            assert page_sizes==[100]*10+[25]
            # Cursor is independent of page size but scoped to the authenticated tenant.
            resumed=await stub.ListDevices(request(17,first_token),timeout=3)
            assert [d.device_id for d in resumed.devices]==ids[100:117]
            for size in [0,101,2**32-1]:
                with pytest.raises(grpc.aio.AioRpcError) as err:
                    await stub.ListDevices(request(size),timeout=3)
                assert err.value.code()==grpc.StatusCode.INVALID_ARGUMENT
            malformed=['!', 'a'*257, 'v1..', first_token+'=', ' '+first_token,
                       'v2.'+first_token.split('.',1)[1], first_token.rsplit('.',1)[0]+'.'+'A'*43,
                       'v1.'+base64.urlsafe_b64encode((tenant+'\n'+ids[500]).encode()).decode().rstrip('=')+'.'+first_token.rsplit('.',1)[1]]
            for bad in malformed:
                with pytest.raises(grpc.aio.AioRpcError) as err:
                    await stub.ListDevices(request(100,bad),timeout=3)
                assert err.value.code()==grpc.StatusCode.INVALID_ARGUMENT
            with pytest.raises(grpc.aio.AioRpcError) as err:
                await north.ListDevices(request(100,first_token),timeout=3)
            assert err.value.code()==grpc.StatusCode.INVALID_ARGUMENT
            own=await north.ListDevices(request(100),timeout=3)
            assert [d.device_id for d in own.devices]==[ids[0]] and not own.next_page_token
            # Empty legacy request decodes with page_size=0 and must fail, not truncate.
            legacy=c.unary_unary('/relay.v1.Fleet/ListDevices',request_serializer=pb.Empty.SerializeToString,response_deserializer=pb.Devices.FromString)
            with pytest.raises(grpc.aio.AioRpcError) as err:await legacy(pb.Empty(),timeout=3)
            assert err.value.code()==grpc.StatusCode.INVALID_ARGUMENT
            observations.update(registered=1025,page_counts=page_sizes,response_bytes=response_bytes,
                                maximum_response_bytes=max(response_bytes),client_receive_limit_unchanged=4*1024*1024,
                                cursor_rejections=len(malformed)+1,invalid_page_sizes=3,legacy_empty_rejected=True)
        fleet.stop();fleet.start();await fleet.ready()
        async with fleet.channel('unregistered') as c:
            resumed=await rpc.FleetStub(c).ListDevices(request(17,first_token),timeout=3)
            assert [d.device_id for d in resumed.devices]==ids[100:117]
        observations['cursor_survived_server_restart']=True

    try:
        asyncio.run(run())
        args=[str(ROOT/'.venv/bin/python'),'-m','agent.operator','--pki',str(fleet.pki),
              '--address',fleet.address,'--identity','unregistered','list','--page-size','37']
        result=subprocess.run(args,cwd=ROOT,capture_output=True,text=True,check=True,timeout=30)
        devices=json.loads(result.stdout)['devices']
        assert [d['device_id'] for d in devices]==ids
        assert all(d['tenant']==tenant for d in devices)
        observations.update(cli_devices=len(devices),cli_page_size=37,passed=True)
        (ROOT/'evidence/pagination-regression.json').write_text(json.dumps(observations,indent=2)+'\n')
    finally:fleet.close()
