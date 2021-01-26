import os
from dotenv import load_dotenv
from typing import List, Optional
from ..auth import Auth
from ..models.contxt import (
    Cluster,
)
from .api import ApiEnvironment, ConfiguredApi
from requests.exceptions import HTTPError

load_dotenv()

class ContxtDeploymentService(ConfiguredApi):
    """Contxt API client"""

    _envs = (
        ApiEnvironment(
            name="production",
            base_url="https://contxt.api.ndustrial.io/deploy/v1",
            client_id="8qY2xJob1JAxhmVhIDLCNnGriTM9bct8",
        ),
        ApiEnvironment(
            name="staging",
            base_url="https://contxt-api.staging.ndustrial.io/deploy/v1",
            client_id="qGzdTXcmB57zlTp86rYsivG9qEss1lbF",
        ),
    )

    def __init__(self, auth: Auth, env: str = os.getenv("env", "production"), **kwargs) -> None:
        super().__init__(env=env, auth=auth, **kwargs)

    def get_clusters(self, organization_id: str) -> List[Cluster]:
        return [Cluster.from_api(rec) for rec in self.get(f"{organization_id}/clusters")]

    def get_cluster(self, organization_id: str, cluster_slug: str) -> Cluster:
        resp = self.get(f"{organization_id}/clusters/{cluster_slug}")
        return Cluster.from_api(resp)

    def register_cluster(self, organization_id: str, cluster: Cluster, secret_bearer_token: Optional[str]) -> Cluster:
        obj = cluster.post()
        # for legacy / DC/OS cluster registration, a token should be provided
        if secret_bearer_token:
            obj["secrets"] = {"BEARER_TOKEN": secret_bearer_token}
        try:
            resp = self.post(f"{organization_id}/clusters", json=obj)
            return Cluster.from_api(resp)
        except HTTPError as e:
            pass
