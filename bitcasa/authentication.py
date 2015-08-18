import logging
import requests

from requests import RequestException
from uuid import uuid4
from threading import Lock

from ghost import Ghost

from .exceptions import AuthenticationError, ConnectionError
from .globals import BITCASA, logger

class AuthenticationManager(object):
    _session = None
    _cookies = None
    _connected = None
    id = None

    def __init__(self, username=None, password=None, cookies=None, auto_open=True):
        self.id = uuid4().hex
        self._connected = False
        self._username = username
        self._password = password
        self._cookies = cookies

        if not any(((username, password), cookies)):
            message = 'Specify either username and password or cookies'
            raise AuthenticationError(message, username=username,
                                      password=password, cookies=cookies)
        if auto_open:
            self.open_session()

        self.request_lock = Lock()

    def assert_valid_session(self):
        if not all((self._session, self._cookies)):
            raise ConnectionError('Invalid session. Did you open one?')

    def logout(self):
        if self._cookies:
            data = {'csrf_token': self._cookies.get('tkey_csrf0portal')}
            self.request(BITCASA.ENDPOINTS.logout, method='POST', data=data)

    def make_download_request(self, url, ignore_session_state=False):
        self.request_lock.acquire()
        if not ignore_session_state:
            self.assert_valid_session()

        if not self._connected:
            kwargs.setdefault('cookies', self._cookies)

        resp = None
        error = None
        try:
            resp = self._session.get(url, stream=True, timeout=120)
            resp.raise_for_status()
        except (ValueError, RequestException):
            error = str(resp.status_code) if resp else 'unknown'

        if error is not None:
            if error == 'unauthorized':
                self._connected = False

            self.request_lock.release()
            raise ConnectionError(error_message % response_data.get('error'))

        if not self._connected:
            self._connected = True

        return resp


    def request(self, endpoint, method='GET', ignore_session_state=False,
                **kwargs):

        self.request_lock.acquire()
        if not ignore_session_state:
            self.assert_valid_session()

        if not self._connected:
            kwargs.setdefault('cookies', self._cookies)

        url = BITCASA.url_from_endpoint(endpoint)

        error_message = 'Error connecting to drive.bitcasa.com. %s'
        response_data = {}
        resp = None
        try:
            resp = self._session.request(method.upper(), url, **kwargs)
            resp.raise_for_status()
            response_data = resp.json()
        except (ValueError, RequestException):
            error_code = str(resp.status_code) if resp else 'unknown'
            response_data.setdefault('error', error_code)

        error = response_data.get('error')
        if error is not None:
            if error == 'unauthorized':
                self._connected = False

            self.request_lock.release()
            raise ConnectionError(error_message % response_data.get('error'))

        if not self._connected:
            self._connected = True

        self.request_lock.release()
        # Copy to prevent memory leak
        return response_data.copy()

    def set_cookies(self):

        ghost = Ghost(log_level=logging.DEBUG)
        full_url = os.path.join(BITCASA.BASE_URL,
                                BITCASA.ENDPOINTS.login.lstrip('/'))
        page, res = ghost.open(full_url)
        import pdb; pdb.set_trace()

        result, res = ghost.set_field_value('input[name="user"]',
                                            self._username)
        result, res = ghost.set_field_value('input[name="password"]',
                                            self._password)

        result, res = ghost.click('input[type="submit"]')
        page, resources = ghost.wait_for_page_loaded()

        # wait for the page to load.
        result, resources = ghost.wait_for_selector('div.account-dropdown', timeout=10)
        if not result:
            ghost.capture_to('bitcasa_login.png')

            # logout just in case.
            full_url = os.path.join(BITCASA.BASE_URL,
                                    BITCASA.ENDPOINTS.logout.lstrip('/'))
            page, res = ghost.open(full_url)

            raise AuthenticationError('login failed', sess=sess,
                                      username=self._username,
                                      password=self._password)

        # Don't keep these around longer than needed.
        self._username = None
        self._password = None

        cookies = ghost.cookies

        parsed_cookies = {}
        for cookie in cookies:
            parsed_cookie = cookie.split(';')
            parsed_cookie = parsed_cookie[0].strip().split('=')
            parsed_cookies[parsed_cookie[0]] = parsed_cookie[1]

        self._cookies = parsed_cookies

    def get_cookies(self):
        return self._cookies

    def open_session(self, reconnect=False, auto_test=True):
        if not reconnect and self._session:
            return self._session

        if not self._cookies:
            self.set_cookies()

        self._session = requests.Session()
