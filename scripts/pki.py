#!/usr/bin/env python3
"""Generate ephemeral local-test PKI; private keys never belong in source control."""
import argparse
import datetime as dt
import ipaddress
import json
from pathlib import Path
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID

def create_pki(path):
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    now = dt.datetime.now(dt.timezone.utc)
    def key(): return rsa.generate_private_key(public_exponent=65537, key_size=2048)
    def name(n): return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,n)])
    def issue(n, k, issuer, signing_key, ca=False, server=False):
        b = (x509.CertificateBuilder().subject_name(name(n)).issuer_name(issuer).public_key(k.public_key())
             .serial_number(x509.random_serial_number()).not_valid_before(now-dt.timedelta(minutes=5))
             .not_valid_after(now+dt.timedelta(days=7)).add_extension(x509.BasicConstraints(ca=ca,path_length=0 if ca else None),critical=True))
        if not ca:
            b=b.add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH if server else ExtendedKeyUsageOID.CLIENT_AUTH]),critical=False)
            if server:
                b=b.add_extension(x509.SubjectAlternativeName([x509.DNSName('localhost'),x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]),critical=False)
        return b.sign(signing_key,hashes.SHA256())
    ca_key=key(); ca=issue('relay-local-ca',ca_key,name('relay-local-ca'),ca_key,ca=True)
    (path/'ca.crt').write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    manifest={}
    for n in ['server','operator-north','operator-south','sim-a','sim-b','south-a','unregistered','bench','replay','rogue']:
        k=key()
        c=issue(n,k,ca.subject,ca_key,server=n=='server') if n!='rogue' else issue(n,k,name(n),k)
        (path/f'{n}.crt').write_bytes(c.public_bytes(serialization.Encoding.PEM))
        private=path/f'{n}.key'
        private.write_bytes(k.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))
        private.chmod(0o600)
        manifest[n]=c.fingerprint(hashes.SHA256()).hex()
    (path/'fingerprints.json').write_text(json.dumps(manifest,indent=2)+'\n')
    return manifest

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('directory',type=Path)
    print(json.dumps(create_pki(p.parse_args().directory),indent=2))
