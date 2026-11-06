import argparse
import asyncio
from pathlib import Path
import signal
from .runtime import run_agent

def main():
    p=argparse.ArgumentParser(description='Simulated device; no physical hardware')
    p.add_argument('--address',default='127.0.0.1:5443');p.add_argument('--pki',type=Path,required=True)
    p.add_argument('--identity',required=True);p.add_argument('--device',required=True)
    p.add_argument('--spool',type=Path,required=True);p.add_argument('--samples',type=int,default=120)
    p.add_argument('--capacity',type=int,default=32);p.add_argument('--interval',type=float,default=.02)
    p.add_argument('--deadline',type=float,default=1);p.add_argument('--clock-skew-ms',type=int,default=0)
    args=p.parse_args()
    if args.samples<0 or args.interval<=0 or args.deadline<=0:p.error('samples >= 0; interval/deadline > 0 required')
    async def run():
        stop=asyncio.Event()
        for sig in (signal.SIGTERM,signal.SIGINT):asyncio.get_running_loop().add_signal_handler(sig,stop.set)
        await run_agent(address=args.address,pki=args.pki,identity=args.identity,device=args.device,spool_path=args.spool,
                        samples=args.samples,capacity=args.capacity,interval=args.interval,deadline=args.deadline,
                        clock_skew_ms=args.clock_skew_ms,stop=stop)
    asyncio.run(run())

if __name__=='__main__':main()
