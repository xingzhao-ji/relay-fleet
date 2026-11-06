"""Tenant-scoped operator CLI using the same generated/mTLS API as devices."""
import argparse
import asyncio
import json
from pathlib import Path
from google.protobuf.json_format import MessageToDict
import grpc
from .generated import fleet_pb2 as pb, fleet_pb2_grpc as rpc

async def list_devices(stub,page_size):
    result=pb.Devices();token='';seen=set()
    while True:
        page=await stub.ListDevices(pb.ListDevicesRequest(page_size=page_size,page_token=token),timeout=3)
        result.devices.extend(page.devices)
        token=page.next_page_token
        if not token:return result
        if token in seen:raise RuntimeError('server repeated a listing cursor; refusing an incomplete result')
        seen.add(token)

async def run(args):
    pki=args.pki
    cred=grpc.ssl_channel_credentials((pki/'ca.crt').read_bytes(),(pki/f'{args.identity}.key').read_bytes(),(pki/f'{args.identity}.crt').read_bytes())
    async with grpc.aio.secure_channel(args.address,cred) as channel:
        s=rpc.FleetStub(channel)
        if args.command=='list':result=await list_devices(s,args.page_size)
        elif args.command=='register':
            fp=json.loads((pki/'fingerprints.json').read_text())[args.certificate]
            result=await s.Register(pb.RegisterRequest(device_id=args.device,certificate_sha256=fp),timeout=3)
        elif args.command=='config':
            result=await s.SetDesired(pb.DesiredRequest(device_id=args.device,expected_version=args.expected,json=json.dumps({'sample_interval_ms':args.interval_ms},separators=(',',':'))),timeout=3)
        elif args.command=='revoke':result=await s.Revoke(pb.DeviceId(device_id=args.device),timeout=3)
        else:result=await s.QueryTelemetry(pb.QueryRequest(device_id=args.device,after_sequence=args.after,limit=args.limit),timeout=3)
        print(json.dumps(MessageToDict(result,preserving_proto_field_name=True,always_print_fields_with_no_presence=True),indent=2))

def main():
    p=argparse.ArgumentParser(description='Certificate-scoped fleet operator')
    p.add_argument('--pki',type=Path,required=True);p.add_argument('--identity',default='operator-north');p.add_argument('--address',default='127.0.0.1:5443')
    sub=p.add_subparsers(dest='command',required=True)
    listing=sub.add_parser('list');listing.add_argument('--page-size',type=int,default=100)
    r=sub.add_parser('register');r.add_argument('device');r.add_argument('certificate')
    c=sub.add_parser('config');c.add_argument('device');c.add_argument('--expected',type=int,required=True);c.add_argument('--interval-ms',type=int,required=True)
    q=sub.add_parser('query');q.add_argument('device');q.add_argument('--after',type=int,default=0);q.add_argument('--limit',type=int,default=32)
    v=sub.add_parser('revoke');v.add_argument('device')
    args=p.parse_args()
    if args.command=='list' and not 1<=args.page_size<=100:p.error('--page-size must be 1..100')
    try:asyncio.run(run(args))
    except grpc.aio.AioRpcError as e:p.exit(1,f'{e.code().name}: {e.details()}\n')
    except RuntimeError as e:p.exit(1,f'{e}\n')

if __name__=='__main__':main()
