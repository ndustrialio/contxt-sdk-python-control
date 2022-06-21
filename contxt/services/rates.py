from typing import Dict, List, Optional, Iterable

from ..models.rates import UtilityProvider, RateSchedule
from .api import ConfiguredLegacyApi
from ..utils.config import ContxtEnvironmentConfig
from .pagination import PagedRecords, PageOptions


class UtilityRatesService(ConfiguredLegacyApi):
    """Utility Rates API client"""

    def __init__(self, env_config: ContxtEnvironmentConfig, **kwargs) -> None:
        super().__init__(env_config=env_config, **kwargs)

    def get_providers(
            self,
            page_options: Optional[PageOptions] = None
    ) -> Iterable[UtilityProvider]:
        return PagedRecords(
            api=self,
            url='providers',
            params={},
            options=page_options,
            record_parser=UtilityProvider.from_api,
            is_v2=True
        )

    def get_schedules_for_provider(
            self,
            utility_provider_id: int,
            page_options: Optional[PageOptions] = None
    ) -> Iterable[RateSchedule]:
        return PagedRecords(
            api=self,
            url='schedules',
            params={'utility_provider_id': utility_provider_id},
            options=page_options,
            record_parser=RateSchedule.from_api,
            is_v2=True
        )

    def get_schedule(self, rate_schedule_id: int) -> RateSchedule:
        resp = self.get(f'schedules/{rate_schedule_id}')
        return RateSchedule.from_api(resp)

    def get_schedules_for_facility(
            self,
            facility_id: int,
            page_options: Optional[PageOptions] = None
    ) -> Iterable[RateSchedule]:
        return PagedRecords(
            api=self,
            url=f'facilities/{facility_id}/schedules',
            params={},
            options=page_options,
            record_parser=RateSchedule.from_api,
            is_v2=True
        )
