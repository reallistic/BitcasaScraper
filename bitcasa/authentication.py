import dryscrape
import requests

from requests import RequestException
from uuid import uuid4
from threading import Lock

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

    def make_download_request(self, endpoint, seek=None,
                              ignore_session_state=False):
        self.request_lock.acquire()
        if not ignore_session_state:
            self.assert_valid_session()

        resp = None
        error = None
        headers = None
        cookies = None
        response_data = {}

        if not self._connected:
            cookies = self._cookies

        url = BITCASA.url_from_endpoint(endpoint)

        if seek:
            headers = {'Range': 'bytes=%s-' % seek}
            logger.debug('Requesting url with seek: %s %s', seek, url)
        else:
            logger.debug('Requesting url %s', url)

        try:
            resp = self._session.get(url, stream=True, timeout=60,
                                     headers=headers, cookies=cookies)
            resp.raise_for_status()
        except (ValueError, RequestException) as e:
            error = resp.content[:30] if resp is not None else e
            logger.exception('%s - %s', error, url)

        if error is not None:
            if error == 'unauthorized':
                self._connected = False

            self.request_lock.release()
            message = 'Error making download request %s'
            raise ConnectionError(message % error)

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
        logger.debug('Requesting url %s', url)
        try:
            resp = self._session.request(method.upper(), url, **kwargs)
            response_data = resp.json()
            resp.raise_for_status()
        except (ValueError, RequestException):
            error = resp.content if resp is not None else 'unknown'
            response_data.setdefault('error', error)

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
        sess = dryscrape.Session(base_url=BITCASA.BASE_URL)

        sess.set_attribute('auto_load_images', False)
        sess.visit(BITCASA.ENDPOINTS.login)

        username = sess.at_xpath('//input[@name="user"]')
        password = sess.at_xpath('//input[@name="password"]')
        username.set(self._username)
        password.set(self._password)
        submit = sess.at_xpath('//input[@type="submit"]')
        submit.click()

        # wait for the page to load.
        listing_items = sess.at_css('div.listing-items', timeout=5)
        if not listing_items:
            sess.render('bitcasa_login.png')
            raise AuthenticationError('login failed', sess=sess,
                                      username=self._username,
                                      password=self._password)

        # Don't keep these around longer than needed.
        self._username = None
        self._password = None

        cookies = sess.cookies()

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
