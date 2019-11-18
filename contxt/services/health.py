from contxt.auth import Auth
from contxt.models.health import Health
from contxt.services.api import ApiEnvironment, ConfiguredApi
from contxt.utils import make_logger

logger = make_logger(__name__)


class HealthService(ConfiguredApi):
    """
    Service to interact with our Health API.
    """

    _envs = (
        ApiEnvironment(
            name="production",
            base_url="https://health.api.ndustrial.io/v1",
            client_id="6uaQIV1KnnWhXiTm09iGDvy2aQaz2xVI",
        ),
    )

    def __init__(self, auth: Auth, env: str = "production", **kwargs) -> None:
        super().__init__(env=env, auth=auth, **kwargs)

    def report(self, org_id, asset_id, health: Health) -> Health:
        data = health.post()
        logger.debug(
            (
                f"Created health status with org_id {org_id} "
                f"asset_id {asset_id} and with data {data}"
            )
        )
        resp = self.post(f"{org_id}/assets/{asset_id}", data=data)
        return Health.from_api(resp)
