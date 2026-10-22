#!/usr/bin/env python3
"""Package current server sources, including uncommitted fixes, without credentials."""
import hashlib
import io
import json
from pathlib import Path
import tarfile


ROOT = Path(__file__).resolve().parents[1]
FILES = [
    'docker-compose.server.yml', 'docker-compose.firebase.yml',
    'deploy/Caddyfile', 'deploy/README.md',
    'api/Dockerfile', 'api/.dockerignore', 'api/package.json', 'api/package-lock.json',
    'api/server.js', 'api/socket-session.js', 'api/mobile.js', 'api/mobile-schema.sql', 'api/push.js', 'api/web-push.js', 'api/organization.js', 'api/calls.js', 'api/app-release.js',
    'scripts/init_server.py', 'scripts/backup_db.sh', 'scripts/configure_web_push.py',
    'scripts/prepare_server_bundle.py', 'scripts/live_server_check.mjs', 'scripts/publish_apk.sh',
]


def main():
    paths = [ROOT / name for name in FILES]
    paths += sorted((ROOT / 'db').rglob('*.sql'))
    web_extensions = {'.html', '.js', '.css', '.svg', '.png', '.webmanifest', '.woff', '.woff2'}
    paths += sorted(p for p in (ROOT / 'web').rglob('*') if p.is_file() and p.suffix in web_extensions)
    hashes, payloads = {}, {}
    for path in paths:
        if path.is_symlink() or not path.is_file():
            raise RuntimeError(f'Required regular source file missing: {path}')
        relative = path.relative_to(ROOT).as_posix()
        payload = path.read_bytes()
        if path.suffix not in {'.png', '.woff', '.woff2'}:
            payload = payload.replace(b'\r\n', b'\n')
        payloads[relative] = payload
        hashes[relative] = hashlib.sha256(payload).hexdigest()
    output = ROOT / 'builds' / 'messenger67-server.tar.gz'
    output.parent.mkdir(exist_ok=True)
    with tarfile.open(output, 'w:gz') as archive:
        for relative, payload in payloads.items():
            info = tarfile.TarInfo(relative)
            info.size = len(payload)
            info.mode = 0o755 if relative.endswith('.sh') else 0o644
            archive.addfile(info, io.BytesIO(payload))
        manifest = json.dumps({'files': hashes}, ensure_ascii=False, indent=2).encode('utf-8')
        info = tarfile.TarInfo('source-manifest.json')
        info.size, info.mode = len(manifest), 0o644
        archive.addfile(info, io.BytesIO(manifest))
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    output.with_suffix(output.suffix + '.sha256').write_text(f'{digest}  {output.name}\n', encoding='ascii')
    print(f'{output}: {len(paths)} source files; SHA256 {digest}')


if __name__ == '__main__':
    main()
