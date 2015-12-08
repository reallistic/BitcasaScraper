class BitcasaError(Exception):
    def __init__(self, message=None, error=None, *args):
        self.args = args
        self.message = message
        self.error = error

class AuthenticationError(BitcasaError):
    def __init__(self, *args, **kwargs):
        self.sess = kwargs.pop('sess', None)
        self.username = kwargs.pop('username', None)
        self.cookies = kwargs.pop('cookies', None)
        super(AuthenticationError, self).__init__(*args, **kwargs)

class ConnectionError(BitcasaError):
    pass

class ConfigError(BitcasaError):
    pass

class SizeMismatchError(BitcasaError):
    pass

class ResponseError(BitcasaError):
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs.pop('error', None)
        self.response = kwargs.pop('response', None)
        super(ResponseError, self).__init__(*args, **kwargs)

class DownloadError(BitcasaError):
    def __init__(self, *args, **kwargs):
        self.item = kwargs.pop('item', None)
        super(DownloadError, self).__init__(*args, **kwargs)
