class BITCASA(object):
    BASE_URL = 'https://drive.bitcasa.com'

    class ENDPOINTS(object):
        download = 'download/v2'
        login = '/login'
        logout = '/portal/logout'
        user_account = '/portal/useraccount'
        root_folder = '/portal/v2/folders/' #?media-metadata=true'

    @classmethod
    def url_from_endpoint(cls, endpoint):
        return '%s%s' % (cls.BASE_URL, endpoint)
