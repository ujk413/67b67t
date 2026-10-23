"""Reject private runtime/build material in the Git index; never read its contents."""
import re
import subprocess
import sys


def forbidden(name):
    name = name.replace('\\', '/')
    if name == '.env.example' or (name.startswith('mobile/src/data/') and name.endswith(('.ts', '.tsx'))):
        return False
    return bool(re.search(
        r'(^|/)(?:\.env(?:\..*)?|secrets?|data|builds|backups?|uploads|node_modules)(?:/|$)'
        r'|\.(?:db(?:-.*)?|sqlite3?(?:-.*)?|dump|backup|keystore|jks|pem|key|p12|pfx|apk|aab|zip|tar|tgz|tar\.gz)$'
        r'|(^|/)(?:google-services\.json|GoogleService-Info\.plist|key\.properties|[^/]*service[-_]account[^/]*\.json|credentials[^/]*\.json)$'
        r'|^mobile/(?:android|ios)/'
        r'|^mobile/modules/[^/]+/android/(?:build|\.cxx|\.gradle)/', name, re.I))


def main():
    names = subprocess.check_output(['git', 'ls-files', '-z']).decode('utf8').split('\0')
    bad = [name for name in names if name and forbidden(name)]
    for name in bad:
        print('Forbidden tracked path:', name)
    if bad:
        return 1
    print('Tracked-file safety: no private runtime, credentials, databases or build archives.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
