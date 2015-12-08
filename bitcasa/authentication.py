
import logging
import dryscrape
import requests

from requests import RequestException
from uuid import uuid4
from threading import Lock

from .exceptions import AuthenticationError, ConnectionError, ResponseError
from .globals import BITCASA

logger = logging.getLogger(__name__)

class RequestHelper(object):
    auth = None
    url = None
    resp = None

    def __init__(self, auth, validate=True):
        self.auth = auth
        self.validate = validate

    def __enter__(self):
        self.auth.request_lock.acquire()
        if self.validate:
            self.auth.assert_valid_session()

        return self

    def send(self, method, url, raw=False, **kwargs):
        self.url = url

        if not self.auth._connected:
            kwargs.setdefault('cookies', self.auth._cookies)

        resp = self.auth._session.request(method.upper(), url, **kwargs)
        resp.raise_for_status()

        if raw:
            return resp
        else:
            if '/json' in resp.headers['content-type']:
                json = resp.json()
                if json.get('error', None):
                    raise ResponseError('Error found in response',
                                        error=json.get('error'),
                                        response=resp)
                return json

            return resp.content

    def __exit__(self, exc_type, exc_value, tb):
        url = self.url
        self.url = None
        self.auth.request_lock.release()

        if exc_type is not None:
            logger.error('Error making request to %s', url,
                         exc_info=(exc_type, exc_value, tb))
            raise exc_value
        elif not self.auth._connected:
            self.auth._connected = True



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
        headers = None

        url = BITCASA.url_from_endpoint(endpoint)

        if seek:
            headers = {'Range': 'bytes=%s-' % seek}
            logger.debug('Requesting url with seek: %s %s', seek, url)
        else:
            logger.debug('Requesting url %s', url)

        validate = not ignore_session_state

        with RequestHelper(self, validate=validate) as req:
            return req.send('GET', url, raw=True, timeout=120,
                            headers=headers)

    def request(self, endpoint, method='GET', ignore_session_state=False,
                **kwargs):

        url = BITCASA.url_from_endpoint(endpoint)

        logger.debug('Requesting url %s', url)

        validate = not ignore_session_state
        with RequestHelper(self, validate=validate) as req:
            return req.send(method.upper(), url, **kwargs)

        error_message = 'Error connecting to drive.bitcasa.com. %s'
        response_data = {}
        resp = None
        try:
            resp = self._session.request(method.upper(), url, **kwargs)
            response_data = resp.json()
            resp.raise_for_status()
        except (ValueError, RequestException) as e:
            error = resp.content[:30] if resp is not None else e
            response_data.setdefault('error', error)
            logger.exception('%s - %s', error, url)

        error = response_data.get('error')
        if error is not None:
            if error == 'unauthorized':
                self._connected = False

            self.request_lock.release()
            raise ConnectionError(error_message % response_data.get('error'))

        if not self._connected:
            self._connected = True

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
