import base64
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from pocket48_auth import (AuthenticationUnavailableError, SmsLogin, TokenManager,
                           device_headers, generate_pa)
from pocket48_scraper import Pocket48Client


class SmsAuthTests(unittest.TestCase):
    def test_pa_matches_reference_format(self):
        with mock.patch('pocket48_auth.time.time', return_value=1234.567), mock.patch(
            'pocket48_auth.uuid.uuid4', return_value=mock.Mock(hex='a' * 32)
        ):
            value = base64.b64decode(generate_pa()).decode()
        prefix = '1234567' + 'a' * 32
        digest = hashlib.md5((prefix + 'ejrw9u345h32u@#&%^#G').encode()).hexdigest()
        self.assertEqual(value, prefix + digest + '2021060901')

    def test_sms_success_survives_reload_and_rejection_without_old_token(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = {'pocket48': {'mobile': '13800000000', 'token': 'old'},
                      'storage': {'token_file': str(Path(tmp) / 'token.json')}}
            login = SmsLogin(config)
            identity = device_headers(login.manager)
            with mock.patch.object(login, 'request', side_effect=[
                {'status': 200}, {'status': 200, 'content': {'token': 'new'}},
                {'status': 200, 'content': {'userInfo': {'userId': 'main'}}}
            ]) as request, mock.patch('builtins.print'):
                login.login(input_fn=lambda _: '123456')
            self.assertEqual(request.call_args_list[1].args[1]['loginType'], 'MOBILE_SMS_CODE')
            client = Pocket48Client.__new__(Pocket48Client)
            client.config = config
            client.token_manager = TokenManager(config['storage']['token_file'])
            client._sync_configured_token_cache()
            self.assertEqual(client.token_manager.get_token(), 'new')
            client.token_manager.clear()
            client.token_manager.reload()
            client._sync_configured_token_cache()
            self.assertFalse(client.token_manager.has_token())
            self.assertEqual(device_headers(client.token_manager), identity)

    def test_failed_login_preserves_token_and_does_not_retry_sms(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = {'pocket48': {'mobile': '13800000000'},
                      'storage': {'token_file': str(Path(tmp) / 'token.json')}}
            login = SmsLogin(config)
            login.manager.set_token('existing')
            with mock.patch.object(login, 'request', return_value={
                'status': 500, 'message': '发送过于频繁'
            }) as request:
                with self.assertRaisesRegex(AuthenticationUnavailableError, '发送过于频繁'):
                    login.login(input_fn=mock.Mock(side_effect=AssertionError))
            self.assertEqual(request.call_count, 1)
            self.assertEqual(login.manager.get_token(), 'existing')

    def test_question_verification_sends_answer(self):
        with tempfile.TemporaryDirectory() as tmp:
            login = SmsLogin({'pocket48': {'mobile': '13800000000'},
                              'storage': {'token_file': str(Path(tmp) / 'token.json')}})
            replies = iter(['答案A', '123456'])
            with mock.patch.object(login, 'request', side_effect=[
                {'status': 2001, 'message': json.dumps({'question': '问题', 'answer': ['答案A']})},
                {'status': 200}, {'status': 200, 'content': {'token': 'new'}},
                {'status': 200, 'content': {'userInfo': {'userId': 'main'}}}
            ]) as request, mock.patch('builtins.print'):
                login.login(input_fn=lambda _: next(replies))
            self.assertEqual(request.call_args_list[1].args[1]['answer'], '答案A')

    def test_request_refreshes_pa_and_uses_timeout(self):
        with tempfile.TemporaryDirectory() as tmp:
            login = SmsLogin({'storage': {'token_file': str(Path(tmp) / 'token.json')}})
            with mock.patch.object(login.session, 'post') as post:
                post.return_value.json.return_value = {'status': 200}
                login.request('/test', {})
                login.request('/test', {})
            first, second = post.call_args_list
            self.assertNotEqual(first.kwargs['headers']['pa'], second.kwargs['headers']['pa'])
            self.assertEqual(first.kwargs['timeout'], 30)

    def test_switch_accounts_verifies_identity_before_saving(self):
        for current, target, chosen in [('main', 'small', '2'), ('small', 'main', '2')]:
            with self.subTest(current=current), tempfile.TemporaryDirectory() as tmp:
                login = SmsLogin({'storage': {'token_file': str(Path(tmp) / 'token.json')}})
                login.manager.set_token('original')
                profile = {'status': 200, 'content': {
                    'userInfo': {'userId': current, 'nickname': current},
                    'bigSmallInfo': {'bigUserInfo': {'userId': 'main'},
                                     'smallUserInfo': [{'userId': 'small'}, {'userId': 'small'}]}}}
                with mock.patch.object(login, 'request', side_effect=[
                    profile, {'status': 200, 'content': {'token': 'switched'}},
                    {'status': 200, 'content': {'userId': target, 'nickname': target}}
                ]) as request, mock.patch('builtins.print'):
                    login.switch_account(input_fn=lambda _: chosen)
                self.assertEqual(request.call_args_list[1].args[1], {'toUserId': target})
                self.assertEqual(request.call_args_list[1].kwargs['token'], 'original')
                self.assertEqual(request.call_args.kwargs['token'], 'switched')
                cache = TokenManager(str(Path(tmp) / 'token.json'))
                self.assertEqual(cache.get_token(), 'switched')
                self.assertEqual(cache.token_data['user_id'], target)

    def test_switch_failure_preserves_cache(self):
        failures = [
            [{'status': 403, 'message': 'token expired'}],
            [{'status': 200, 'content': {}}],
            [{'status': 200, 'content': {'token': 'wrong'}},
             {'status': 200, 'content': {'userId': 'main'}}],
        ]
        for failure in failures:
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                login = SmsLogin({'storage': {'token_file': str(Path(tmp) / 'token.json')}})
                login.manager.set_token('original')
                original = login.manager.token_file.read_bytes()
                profile = {'status': 200, 'content': {'userInfo': {'userId': 'main'},
                           'bigSmallInfo': {'smallUserInfo': [{'userId': 'small'}]}}}
                with mock.patch.object(login, 'request', side_effect=[profile, *failure]), mock.patch('builtins.print'):
                    with self.assertRaises(AuthenticationUnavailableError):
                        login.switch_account(input_fn=lambda _: '2')
                self.assertEqual(login.manager.token_file.read_bytes(), original)

    def test_keep_current_and_cancel_do_not_call_switch_endpoint(self):
        for answer in ['', 'q']:
            with self.subTest(answer=answer), tempfile.TemporaryDirectory() as tmp:
                login = SmsLogin({'storage': {'token_file': str(Path(tmp) / 'token.json')}})
                login.manager.set_token('original')
                original = login.manager.token_file.read_bytes()
                profile = {'status': 200, 'content': {'userId': 'main',
                           'bigSmallInfo': {'smallUserInfo': [{'userId': 'small'}]}}}
                replies = iter(['invalid', answer])
                with mock.patch.object(login, 'request', return_value=profile) as request, mock.patch('builtins.print'):
                    login.switch_account(input_fn=lambda _: next(replies))
                self.assertEqual(request.call_count, 1)
                self.assertEqual(login.manager.get_token(), 'original')
                if answer == 'q':
                    self.assertEqual(login.manager.token_file.read_bytes(), original)

    def test_missing_login_cannot_switch(self):
        with tempfile.TemporaryDirectory() as tmp:
            login = SmsLogin({'storage': {'token_file': str(Path(tmp) / 'token.json')}})
            with mock.patch.object(login, 'request') as request:
                with self.assertRaisesRegex(AuthenticationUnavailableError, '--login'):
                    login.switch_account()
                request.assert_not_called()

    def test_login_keeps_recovery_token_when_account_switch_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            login = SmsLogin({'pocket48': {'mobile': '13800000000'},
                              'storage': {'token_file': str(Path(tmp) / 'token.json')}})
            login.manager.set_token('existing')
            self.assertEqual(login.session.headers['P-Sign-Type'], 'V0')
            with mock.patch.object(login, 'request', side_effect=[
                {'status': 200}, {'status': 200, 'content': {'token': 'fresh-main'}}
            ]), mock.patch.object(login, 'select_account', side_effect=AuthenticationUnavailableError('切换失败')):
                with self.assertRaises(AuthenticationUnavailableError):
                    login.login(input_fn=lambda _: '123456')
            self.assertEqual(login.manager.get_token(), 'existing')
            with mock.patch.object(login, 'select_account') as select:
                login.switch_account()
                self.assertEqual(select.call_args.args[0], 'fresh-main')
            pending = TokenManager(str(Path(tmp) / 'pending_login.json'))
            self.assertFalse(pending.has_token())
