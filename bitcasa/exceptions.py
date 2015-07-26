class BitcasaError(Exception):
    pass

class AuthenticationError(BitcasaError):
    def __init__(self, *args, **kwargs):
        self.sess = kwargs.pop('sess', None)
        self.username = kwargs.pop('username', None)
        self.password = kwargs.pop('password', None)
        super(AuthenticationError, self).__init__(*args, **kwargs)

class ConnectionError(BitcasaError):
    pass
