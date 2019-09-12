import lookerapi as looker

class Looker:
    """Looker api class
    """
    def __init__(self, base_url, client_id, client_secret):
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.unauthenticated_client = looker.ApiClient(base_url)
        self.unauthenticated_authApi = looker.ApiAuthApi(unauthenticated_client)
        self.token = None
        self.client = None
        self.api = None

    def _authenticate_client(self):
        self.token = self.unauthenticated_authApi.login(client_id=client_id, client_secret=client_secret)
        self.client = looker.ApiClient(base_url, 'Authorization', 'token ' + token.access_token)

    def get_client(self, authenticate=False):
        if (not self.client) or authenticate:
            self._authenticate_client()
        return self.client

    # instantiate User API client
    def get_api(self):
        if not self.api:
            self.api = looker.LookmlModelApi(self.get_client())

        return self.api