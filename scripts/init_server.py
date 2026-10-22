#!/usr/bin/env python3
"""Create a new deployment's secrets once; never reset an existing database."""
import argparse
import base64
import ipaddress
import os
from pathlib import Path
import re
import secrets


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--domain', default='67chat.ru')
    parser.add_argument('--ip', required=True)
    parser.add_argument('--relay-ip', help='IPv4 on the host NIC, if different from public IP')
    parser.add_argument('--directory', type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    domain = args.domain.lower().rstrip('.')
    if not re.fullmatch(r'(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}', domain):
        parser.error('Use a DNS hostname without protocol, port or path')
    public_ip = ipaddress.IPv4Address(args.ip)
    if not public_ip.is_global:
        parser.error('--ip must be a public IPv4 address')
    relay_ip = ipaddress.IPv4Address(args.relay_ip or args.ip)
    root = args.directory.resolve(strict=True)
    env = root / '.env'
    pg = root / 'data' / 'postgres'
    if env.exists() or env.is_symlink() or (pg.exists() and any(pg.iterdir())):
        parser.error('Existing .env or database found; keep existing keys and configuration')
    values = {
        'SITE_DOMAIN': domain,
        'PUBLIC_IPV4': str(public_ip),
        'TURN_RELAY_IP': str(relay_ip),
        'POSTGRES_PASSWORD': secrets.token_hex(32),
        'ENCRYPTION_KEY': base64.b64encode(secrets.token_bytes(32)).decode('ascii'),
        'TURN_SECRET': secrets.token_hex(32),
        'ADMIN_USERNAMES': '',
        'HCAPTCHA_SECRET': '',
    }
    uploads = root / 'data' / 'uploads'
    uploads.mkdir(parents=True, exist_ok=True)
    if os.name == 'posix':
        if os.geteuid() != 0:
            parser.error('Run with sudo so the uploads directory can belong to UID 1000')
        os.chown(uploads, 1000, 1000)
        os.chmod(uploads, 0o755)
    fd = os.open(env, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as stream:
        stream.write('# Generated for a NEW empty deployment. Keep this file in backups.\n')
        stream.writelines(f'{key}={value}\n' for key, value in values.items())
    print(f'Created {env}; secrets were not printed. Existing data was not imported.')


if __name__ == '__main__':
    main()
