import dryscrape
import requests

from requests import RequestException

from .exceptions import *
from .globals import connection, _connection_stack, BITCASA, logger
from .models import BitcasaUser

class AuthenticationManager(object):
    _session = None
    _cookies = None
    _connected = None

    def __init__(self, username, password, auto_open=True):
        self._connected = False
        self._username = username
        self._password = password
        if auto_open:
            self.open_session()

    def assert_valid_session(self):
        if not all((self._session, self._cookies, self._connected)):
            raise ConnectionError('Not connected')

    def request(self, endpoint, method='GET', ignore_session_state=False,
                **kwargs):
        auto_release_connection = kwargs.pop('auto_release_connection', True)
        if not ignore_session_state:
            self.assert_valid_session()

        url = BITCASA.url_from_endpoint(endpoint)

        try:
            resp = self._session.request(method.upper(), url, **kwargs)
            resp.raise_for_status()
            response_data = resp.json()
        except (ValueError, RequestException):
            if auto_release_connection:
                self.release()
            raise ConnectionError((error_message % '').strip())

        error = response_data.get('error')
        if error:
            if error == 'unauthorized':
                self._connected = False

            raise ConnectionError(error_message % response_data.get('error'))

        if auto_release_connection:
            self.release()

        # Copy to prevent memory leak
        return response_data.copy()

    def release(self):
        logger.info('returning connection')
        _connection_stack.push(self)

    def connect(self):
        error_message = 'Error connecting to drive.bitcasa.com. %s'

        response_data = self.request(BITCASA.ENDPOINTS.user_account,
                                     cookies=self._cookies,
                                     ignore_session_state=True)
        self.user = BitcasaUser.from_account_data(response_data)
        self._connected = True

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

    def open_session(self, reconnect=False, auto_test=True):
        if not reconnect and self._session:
            return self._session

        if not self._cookies:
            self.set_cookies()

        self._session = requests.Session()
        self.connect()
