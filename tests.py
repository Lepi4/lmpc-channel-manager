"""Offline release checks using a temporary database and an in-process HTTP client."""
import asyncio, importlib.util, json, os, pathlib, re, sqlite3, tempfile, unittest
from aiohttp.test_utils import TestClient, TestServer
os.environ['CHANNEL_MANAGER_CONFIG']=str(pathlib.Path(tempfile.gettempdir())/'lmpc-nonexistent-test-config.json')
import channel_manager as cm

class Release(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.db=str(pathlib.Path(self.tmp.name)/'channels.db')
  self.previous_db=cm.CONFIG['db_path'];cm.CONFIG['db_path']=self.db
  cm._first_run_setup(self.db);self.tsb=cm.TsbDaemon(self.db)
  self.client=TestClient(TestServer(cm.make_app(self.tsb)));await self.client.start_server()
 async def asyncTearDown(self):
  await self.client.close();cm.CONFIG['db_path']=self.previous_db;self.tmp.cleanup()
 async def test_no_bundled_accounts_or_sources(self):
  self.assertEqual(cm._DEFAULT_SOURCES,[])
  r=await self.client.get('/api/sources');self.assertEqual((await r.json())['sources'],[])
 async def test_default_audio_normalization_is_off(self):
  self.assertEqual(cm._audio_fix_filter('fixture'),(None,0.0))
 async def test_playlist_and_status_on_clean_database(self):
  r=await self.client.get('/playlist.m3u8');self.assertEqual(r.status,200);self.assertIn('#EXTM3U',await r.text())
  r=await self.client.get('/tsb/status');self.assertEqual(r.status,200);self.assertEqual((await r.json())['total_clients'],0)
 async def test_package_contains_only_program_files(self):
  import build_release,tarfile
  output=pathlib.Path(self.tmp.name)/'dist';build_release.build(output)
  with tarfile.open(output/'channel-manager.tar.gz') as tf:
   names=tf.getnames();self.assertNotIn('payload/config.json',names)
   self.assertFalse(any('.db' in n for n in names));self.assertEqual(len(names),len(build_release.FILES)+1)
 async def test_no_embedded_home_endpoints(self):
  text=pathlib.Path(cm.__file__).read_text(encoding='utf-8')
  self.assertNotRegex(text,r'192\.168\.\d+\.\d+')
  self.assertNotRegex(text,r'https?://[^\s\x22\x27]+/pl/\d+/[a-zA-Z0-9]+')

if __name__=='__main__':unittest.main()
