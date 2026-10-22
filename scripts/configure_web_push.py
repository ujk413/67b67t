#!/usr/bin/env python3
"""Generate this server's VAPID key pair once, without printing private keys."""
import json
import os
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[1]
env = root / '.env'
values = dict(line.split('=', 1) for line in env.read_text().splitlines() if '=' in line and not line.startswith('#'))
present = [bool(values.get(key)) for key in ['WEB_PUSH_PUBLIC_KEY', 'WEB_PUSH_PRIVATE_KEY']]
if any(present):
    if not all(present):
        raise SystemExit('Incomplete VAPID configuration; preserve the existing key and repair it first')
    print('Web Push keys already configured; unchanged')
else:
    script = "const c=require('node:crypto').createECDH('prime256v1');c.generateKeys();process.stdout.write(JSON.stringify({public:c.getPublicKey().toString('base64url'),private:c.getPrivateKey().toString('base64url')}))"
    keys = json.loads(subprocess.check_output(['docker', 'exec', 'messenger67-api', 'node', '-e', script]))
    with env.open('a', encoding='utf-8', newline='\n') as stream:
        stream.write(f"\nWEB_PUSH_PUBLIC_KEY={keys['public']}\nWEB_PUSH_PRIVATE_KEY={keys['private']}\n")
    os.chmod(env, 0o600)
    print('Web Push keys created in server .env; private key was not printed')
