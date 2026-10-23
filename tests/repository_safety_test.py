import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('safety', Path(__file__).resolve().parents[1] / 'scripts/check_tracked_files.py')
safety = importlib.util.module_from_spec(spec)
spec.loader.exec_module(safety)


class RepositorySafety(unittest.TestCase):
    def test_private_material_is_rejected_even_if_force_added(self):
        for name in ['.env', 'api/.env.production', 'data/postgres/db', 'secrets/firebase.json',
                     'mobile/google-services.json', 'key.properties', 'signing/release.keystore',
                     'archive.tar.gz', 'db/real.sqlite-wal', 'backups/users.dump', 'uploads/photo.png',
                     'mobile/android/app/build.gradle', 'firebase_service_account.json', 'mobile/src/data/history.sqlite']:
            with self.subTest(name=name):
                self.assertTrue(safety.forbidden(name))

    def test_templates_schemas_and_mobile_source_remain_publishable(self):
        for name in ['.env.example', 'db/init.sql', 'db/migrations/005_group_e2e.sql',
                     'mobile/src/data/repository.ts', 'web/icons/app-192.png', 'api/package-lock.json']:
            with self.subTest(name=name):
                self.assertFalse(safety.forbidden(name))


if __name__ == '__main__':
    unittest.main()
