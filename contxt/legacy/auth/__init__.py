from datetime import datetime
from json import dump, load
from pathlib import Path

from auth0.v3.authentication import GetToken

from contxt.legacy.services.auth import ContxtAuthService
from contxt.utils import Utils, make_logger
from jwt import decode

logger = make_logger(__name__)


class BaseAuth:
    AUTH_AUDIENCE = "75wT048QcpE7ujwBJPPjr263eTHl4gEX"
    CLI_CLIENT_ID = "bleED0RUwb7CJ9j7D48tqSiSZRZn29AV"
    CLI_CLIENT_SECRET = (
        "0s8VNQ26QrteS3H5KXIIPvkDcNL5PfT-_pWwAVNI4MpDaDg86O2XUH8lT19KLNiZ"
    )

    def __init__(self, client_id, client_secret=None, cli_mode=False):
        self.client_id = client_id
        self.client_secret = client_secret
        self.auth0 = GetToken("ndustrial.auth0.com")

        self.contxt_config_path = Path.home() / ".contxt"
        self.token_file = self.contxt_config_path / "tokens"

        # load up the tokens we have
        self.tokens = self.load_tokens()

        self.auth_access_token = (
            self.get_token_for_audience(self.AUTH_AUDIENCE)
            if self.AUTH_AUDIENCE in self.tokens
            else None
        )

        if not cli_mode:
            self.contxt_auth = ContxtAuthService(
                access_token=self.auth_access_token,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
        else:
            # if there is no token for auth, then we need to go ask the user to login so we can get one
            if not self.auth_access_token:
                self.login()
                self.auth_access_token = (
                    self.get_token_for_audience(self.AUTH_AUDIENCE)
                    if self.AUTH_AUDIENCE in self.tokens
                    else None
                )

            self.contxt_auth = ContxtAuthService(access_token=self.auth_access_token)

    def get_auth_token(self):
        return self.auth_access_token

    def set_auth_token(self, access_token, refresh_token=None):
        self.auth_access_token = access_token
        self.store_service_token(
            audience=self.AUTH_AUDIENCE,
            access_token=access_token,
            refresh_token=refresh_token,
        )

    def refresh_contxt_auth_token(self):
        logger.debug(f"Refreshing contxt auth token")
        refresh_token = self.tokens[self.AUTH_AUDIENCE]["refresh_token"]
        token = self.auth0.refresh_token(
            client_id=self.client_id,
            client_secret=self.client_secret or "",
            refresh_token=refresh_token,
        )

        # store the new access token and re-store the existing refresh token
        self.store_service_token(
            audience=self.AUTH_AUDIENCE,
            access_token=token["access_token"],
            refresh_token=refresh_token,
        )

    def authenticate_to_service(self, service_audience):
        logger.debug(f"Getting new token for {service_audience}")
        token = self.contxt_auth.get_new_token_for_audience(service_audience)
        self.store_service_token(audience=service_audience, access_token=token)
        return token

    def get_token_for_audience(self, audience):
        logger.debug(f"Getting token for client {audience}")
        # check to see if we've gotten a token for this service or not
        if audience not in self.tokens:
            # try to get a token, whether it's a refresh or not
            self.authenticate_to_service(audience)

        # check to see if have the token, but needs to be refreshed
        if self.token_is_expired_for_audience(audience):
            logger.warn(f"Refreshing expired token for client {audience}...")

            # if it's the contxt auth client, we need to follow the other refresh route via Auth0
            if audience == self.AUTH_AUDIENCE:
                self.refresh_contxt_auth_token()
            else:
                self.authenticate_to_service(audience)

        access_token = self.tokens[audience]["token"]
        return access_token

    def token_is_expired_for_audience(self, audience):
        logger.debug(f"Checking if token is expired for audience {audience}")
        if not self.tokens.get(audience):
            return True
        access_token = self.tokens[audience]["token"]
        decoded_token = decode(access_token, verify=False)
        token_expiration_epoch = decoded_token["exp"]

        return token_expiration_epoch <= Utils.get_epoch_time(datetime.now())

    def read_token_file(self):
        logger.debug(f"Loading tokens from {self.token_file}")
        self.contxt_config_path.mkdir(parents=True, exist_ok=True)

        try:
            with self.token_file.open("r") as f:
                return load(f)
        except FileNotFoundError:
            logger.debug("Token file does not yet exist")
            return {}

    def load_tokens(self):
        all_tokens = self.read_token_file()
        return all_tokens.get(self.client_id, {})

    def store_service_token(self, audience, access_token, refresh_token=None):

        self.tokens.setdefault(self.client_id, {})
        # if self.client_id not in self.tokens:
        #     self.tokens[self.client_id] = {}

        self.tokens[audience] = {"token": access_token, "refresh_token": refresh_token}

        self.store_tokens(self.tokens)

    def store_tokens(self, tokens_for_client):
        logger.debug(f"Storing tokens to {self.token_file}")

        all_tokens = self.read_token_file()
        all_tokens[self.client_id] = tokens_for_client

        with self.token_file.open("w") as f:
            dump(all_tokens, f, indent=4)

    def clear_tokens(self):
        logger.debug(f"Removing toke file {self.token_file}")
        self.token_file.unlink()
