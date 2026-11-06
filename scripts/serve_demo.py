"""Create local demonstration credentials once, then run the real Java server."""
import argparse
import json
from pathlib import Path
import subprocess
from .pki import create_pki

ROOT=Path(__file__).resolve().parents[1]

def main():
    p=argparse.ArgumentParser();p.add_argument('--directory',type=Path,default=ROOT/'.runtime/manual');p.add_argument('--port',type=int,default=5443)
    a=p.parse_args();a.directory.mkdir(parents=True,exist_ok=True)
    pki=a.directory/'pki';db=a.directory/'fleet.db';binary=ROOT/'build/install/relay-fleet/bin/relay-fleet'
    if not db.exists():
        fps=create_pki(pki)
        for tenant in ['north','south']:
            subprocess.run([str(binary),'bootstrap',str(db),fps[f'operator-{tenant}'],tenant],check=True)
    print(json.dumps({'pki':str(pki),'database':str(db),'address':f'127.0.0.1:{a.port}','simulation':True}),flush=True)
    server=subprocess.Popen([str(binary),'serve',str(db),str(a.port),str(pki/'server.crt'),str(pki/'server.key'),str(pki/'ca.crt')])
    try:raise SystemExit(server.wait())
    except KeyboardInterrupt:
        server.terminate();server.wait(timeout=8)

if __name__=='__main__':main()
