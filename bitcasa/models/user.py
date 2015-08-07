from . import LaxObject


class BitcasaUser(LaxObject):

    _keys = ['account_id', 'account_state', 'created_at', 'email',
             'first_name', 'last_name', 'id', 'last_login', 'username',
             'syncid', 'storage_limit', 'storage_usage', 'content_base_url']

    @classmethod
    def from_account_data(cls, data):
        user_data = data.get('user', {})
        account_data = data.get('account', {})
        session_data = user_data.get('session', {})
        storage_data = user_data.get('storage', {})

        user = data.get('user', {}).copy()
        user['account_plan'] = user_data.get('account_plan',
                                             {}).get('display_name')
        user['account_state'] = user_data.get('account_state',
                                              {}).get('display_name')
        user['storage_limit'] = storage_data.get('limit')
        user['storage_usage'] = storage_data.get('usage')
        user['syncid'] = session_data.get('syncid')
        user['content_base_url'] = account_data.get('usercontent_domain')

        return cls(**user)
