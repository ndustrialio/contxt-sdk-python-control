from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, FrozenSet, Optional, Tuple

from requests import PreparedRequest, Response, Session
from requests.adapters import HTTPAdapter
from requests.auth import AuthBase
from requests.exceptions import HTTPError
from urllib3.util.retry import Retry

from ..auth import Auth, TokenProvider
from ..utils import make_logger
from ..utils.config import ApiEnvironment, ContxtEnvironmentConfig

logger = make_logger(__name__)

# NOTE: support breaking change in urllib3: https://github.com/urllib3/urllib3/issues/2092
RETRY_METHODS_PARM = (
    "allowed_methods" if hasattr(Retry.DEFAULT, "allowed_methods") else "method_whitelist"
)


class BearerTokenAuth(AuthBase):
    """Bearer token to authorize requests"""

    def __init__(self, token_provider: TokenProvider) -> None:
        self.token_provider = token_provider

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        request.headers["Authorization"] = f"Bearer {self.token_provider.access_token}"
        return request


class ApiRetry(Retry):
    def __init__(
        self,
        total: int = 3,
        backoff_factor: float = 0.1,
        method_whitelist: FrozenSet = frozenset(
            # NOTE: be careful as some of these methods are not idempotent
            ["DELETE", "GET", "OPTIONS", "POST", "PUT", "TRACE", "HEAD"]
        ),
        status_forcelist: Tuple[int, ...] = (500, 502, 504),
        raise_on_status: bool = False,
        **kwargs,
    ) -> None:
        kwargs[RETRY_METHODS_PARM] = method_whitelist
        super().__init__(
            total=total,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            raise_on_status=raise_on_status,
            **kwargs,
        )


class Api:
    """An API with url `baseUrl`.

    If `token_provider` is specified, all requests will be authenticated with
    the access token it provides.
    """

    def __init__(
        self,
        base_url: str,
        token_provider: Optional[TokenProvider] = None,
        retry: Optional[ApiRetry] = ApiRetry(),
    ) -> None:
        self.base_url = base_url if base_url.endswith("/") else f"{base_url}/"

        # Initialize session
        self.session = Session()
        print(token_provider)
        self.session.auth = BearerTokenAuth(token_provider) if token_provider else None
        self.session.headers.update({"Cache-Control": "no-cache"})
        self.session.hooks = {"response": self._log_response}  # type: ignore

        # Attach retry adapter
        if retry:
            adapter = HTTPAdapter(max_retries=retry)
            self.session.mount("http://", adapter)
            self.session.mount("https://", adapter)

    def _url(self, uri: str) -> str:
        return f"{self.base_url}{uri}"

    def _log_response(self, response: Response, *args, **kwargs) -> None:
        t = response.elapsed.total_seconds()
        logger.debug(
            f"Called {response.request.method} {response.url} with body"  # type: ignore
            f" {response.request.body} ({t} s)"
        )

    def _process_response(self, response: Response) -> Dict:
        try:
            # Raise any error
            print(response)
            response.raise_for_status()
        except HTTPError:
            # Catch the error, to log the response's message, and reraise
            # Try to decode the response as json, else fall back to raw text
            response_json = self._get_json(response)
            msg = response_json.get("message") or response_json or response.text
            logger.debug(f"HTTP Error: {response.reason} - {msg}")
            raise

        # Return json, if any
        return self._get_json(response)

    def _get_json(self, response: Response) -> Dict:
        try:
            return response.json()
        except ValueError:
            return {}

    def get(self, uri: str, params: Optional[Dict] = None, **kwargs) -> Dict:
        """Sends a GET request"""
        response = self.session.get(url=self._url(uri), params=params, **kwargs)
        return self._process_response(response)

    def post(self, uri: str, data: Optional[Dict] = None, json: Optional[Dict] = None, **kwargs) -> Dict:
        """Sends a POST request"""
        print('Posting')
        response = self.session.post(url=self._url(uri), data=data, json=json, **kwargs)
        return self._process_response(response)

    def put(self, uri: str, data: Optional[Dict] = None, json: Optional[Dict] = None, **kwargs) -> Dict:
        """Sends a PUT request"""
        response = self.session.put(url=self._url(uri), data=data, json=json, **kwargs)
        return self._process_response(response)

    def delete(self, uri: str, **kwargs) -> Dict:
        """Sends a DELETE request"""
        response = self.session.delete(url=self._url(uri), **kwargs)
        return self._process_response(response)


class EnvironmentException(Exception):
    pass


class ConfiguredLegacyApi(Api, ABC):
    """An `Api` configured for multiple environments, such as staging and production.
    Available environments are expressed by `_envs` and the desired environment is
    specified by its name `env`.

    Overload this class to implement `_envs`.
    """

    def __init__(self, env_config: ContxtEnvironmentConfig, auth: Optional[Auth] = None, **kwargs) -> None:
        # TODO: figure out a cleaner approach to environment selection

        token_provider = auth.get_token_provider(env_config.apiEnvironment.clientId) if auth else None
        super().__init__(base_url=env_config.apiEnvironment.baseUrl, token_provider=token_provider, **kwargs)


class ConfiguredGraphApi(Api, ABC):
    """An `Api` configured for multiple environments, such as staging and production.
    Available environments are expressed by `_envs` and the desired environment is
    specified by its name `env`.

    Overload this class to implement `_envs`.
    """

    def __init__(self, contxt_env: ContxtEnvironmentConfig, auth: Optional[Auth] = None, **kwargs) -> None:
        # TODO: figure out a cleaner approach to environment selection
        self.client_id = contxt_env.clientId
        token_provider = auth.get_token_provider(self.client_id) if auth else None
        super().__init__(base_url=contxt_env.apiEnvironment.baseUrl, token_provider=token_provider, **kwargs)
