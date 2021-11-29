from os import path
import json
from auth0.v3.authentication import GetToken
from sgqlc.operation import Operation
from sgqlc.endpoint.http import HTTPEndpoint
from sgqlc.introspection import query as introspection_query, variables
from sgqlc.codegen.schema import CodeGen, load_schema

from contxt.services.api import ApiEnvironment
from contxt.services.auth import StoredTokenCache


class BaseGraphService:

    def __init__(self, client_id: str, client_secret: str, api_environment: ApiEnvironment,
                 service_name: str):
        self.service_name = service_name
        self.endpoint = None
        self.env = api_environment
        self.url = self.env.baseUrl
        self.client_id = client_id
        self.client_secret = client_secret
        self.audience = self.env.clientId
        self.token_cache = StoredTokenCache()

    def _get_endpoint(self):
        if not self.endpoint:
            self.endpoint = HTTPEndpoint(self.url, {'Authorization': f'Bearer {self.get_auth_token()}'})
        return self.endpoint

    def get_auth_token(self):
        cached_token = self.token_cache.get_token(client_id=self.client_id,
                                                  audience=self.audience)
        if cached_token:
            return cached_token

        if self.env.authRequired:
            print(f'Getting token for {self.client_id}: {self.client_secret} against {self.audience} at'
                  f' auth provider: {self.env.authProvider}')
            req = GetToken(self.env.authProvider)
            token = req.client_credentials(client_id=self.client_id, client_secret=self.client_secret,
                                           audience=self.audience)
            self.token_cache.set_token(client_id=self.client_id,
                                       audience=self.audience,
                                       token=token['access_token'])
            return token['access_token']

    def update_schema(self):
        data = self._get_endpoint()(introspection_query, variables())

        base_file_path = path.dirname(__file__)
        json_schema_filepath = path.abspath(path.join(base_file_path, self.service_name, f"{self.service_name}_schema.json"))
        with open(json_schema_filepath, 'w') as f:
            print(f'Writing schema to {json_schema_filepath}')
            json.dump(data, f, sort_keys=True, indent=2, default=str)

        python_schema_filepath = path.abspath(path.join(base_file_path, self.service_name, f'{self.service_name}_schema.py'))
        print('Generating code for schema')
        with open(json_schema_filepath, 'r') as json_file:
            schema = load_schema(json_file)
            with open(python_schema_filepath, 'w') as schema_file:
                gen = CodeGen(self.service_name, schema, schema_file.write, docstrings=True)
                gen.write()
            print('Schema and types updated!')