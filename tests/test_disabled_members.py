import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from pocket48_scraper import MessageScraper


class DisabledMembersTests(unittest.TestCase):
    def test_disabled_members_retained_in_config_but_never_selected(self):
        scraper = MessageScraper.__new__(MessageScraper)
        members = [{'ownerName': '正常'}, {'ownerName': '停用', 'crawlEnabled': False}]
        scraper.config = {'members': members}
        self.assertEqual(scraper._select_members(), [members[0]])
        with self.assertLogs(level='WARNING'):
            self.assertEqual(scraper._select_members(['停用']), [])
        self.assertEqual(scraper.config['members'], members)
