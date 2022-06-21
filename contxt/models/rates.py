from dataclasses import dataclass, field
from datetime import datetime, date, time, timedelta
from typing import ClassVar, Optional, List

import pandas as pd

from . import ApiField, ApiObject, Parsers


class RTPRateException(Exception):
    pass


@dataclass
class UtilityProvider(ApiObject):
    _api_fields: ClassVar = (
        ApiField("id"),
        ApiField("label", creatable=True),
        ApiField("createdAt", data_type=Parsers.datetime),
        ApiField("updatedAt", data_type=Parsers.datetime),
    )

    id: int
    label: str
    createdAt: Optional[datetime] = None
    updatedAt: Optional[datetime] = None


@dataclass
class RateScheduleApplicability(ApiObject):
    _api_fields: ClassVar = (
        ApiField("id"),
        ApiField("rate_schedule_id"),
        ApiField("min_demand"),
        ApiField("max_demand"),
        ApiField("demand_history"),
        ApiField("min_consumption"),
        ApiField("max_consumption"),
        ApiField("consumption_history"),
        ApiField("min_voltage"),
        ApiField("max_voltage"),
        ApiField("voltage_category"),
        ApiField("phase_wiring"),
    )

    id: int
    rate_schedule_id: int
    min_demand: int
    max_demand: int
    demand_history: str
    min_consumption: int
    max_consumption: int
    consumption_history: str
    min_voltage: int
    max_voltage: int
    voltage_category: str
    phase_wiring: str


@dataclass
class RateScheduleAttribute(ApiObject):
    _api_fields: ClassVar = (
        ApiField("id")
    )

    id: int


@dataclass
class RateScheduleFixedCharge(ApiObject):
    _api_fields: ClassVar = (
        ApiField("id"),
        ApiField("rate_schedule_id"),
        ApiField("name"),
        ApiField("amount"),
        ApiField("createdAt", data_type=Parsers.datetime),
        ApiField("updatedAt", data_type=Parsers.datetime),
    )

    id: int
    rate_schedule_id: int
    name: str
    amount: int
    createdAt: Optional[datetime] = None
    updatedAt: Optional[datetime] = None


@dataclass
class DemandTierRate(ApiObject):
    _api_fields: ClassVar = (
        ApiField("id"),
        ApiField("demand_season_period_id"),
        ApiField("unit_start_value"),
        ApiField("unit_stop_value"),
        ApiField("unit"),
        ApiField("rate"),
        ApiField("adjustment"),
        ApiField("demandSeasonPeriodId"),
    )

    id: int
    demand_season_period_id: int
    demandSeasonPeriodId: int
    unit_start_value: int
    unit_stop_value: int
    unit: str
    rate: int
    adjustment: Optional[int] = None


@dataclass
class DemandSeasonPeriod(ApiObject):
    _api_fields: ClassVar = (
        ApiField("id", data_type=int),
        ApiField("demand_season_id", data_type=int),
        ApiField("day_of_week_start", data_type=int),
        ApiField("hour_start", data_type=int),
        ApiField("minute_start", data_type=int),
        ApiField("day_of_week_end", data_type=int),
        ApiField("hour_end", data_type=int),
        ApiField("minute_end", data_type=int),
        ApiField("period_name"),
        ApiField("demandSeasonId"),
        ApiField("demand_tier_rates", data_type=DemandTierRate),
    )

    id: int
    demand_season_id: int
    day_of_week_start: int
    hour_start: int
    minute_start: int
    day_of_week_end: int
    hour_end: int
    minute_end: int
    period_name: str
    demandSeasonId: int
    demand_tier_rates: List[DemandTierRate]


@dataclass
class DemandSeason(ApiObject):
    _api_fields: ClassVar = (
        ApiField("id"),
        ApiField("rate_schedule_id"),
        ApiField("season_name"),
        ApiField("season_type"),
        ApiField("start_month", data_type=int),
        ApiField("start_day", data_type=int),
        ApiField("end_month", data_type=int),
        ApiField("end_day", data_type=int),
        ApiField("demand_season_periods", data_type=DemandSeasonPeriod)
    )

    id: int
    rate_schedule_id: int
    season_name: str
    season_type: str
    start_month: int
    start_day: int
    end_month: int
    end_day: int
    demand_season_periods: List[DemandSeasonPeriod]

    def rates_for_date(self, for_date: date) -> pd.DataFrame:
        rates = pd.DataFrame()
        for period in self.demand_season_periods:
            if period.day_of_week_start <= for_date.isoweekday() - 1 <= period.day_of_week_end:
                if len(period.demand_tier_rates) > 1:
                    print('Warning -- multiple rates!')
                rate = period.demand_tier_rates[0]
                start = datetime.combine(for_date, time(hour=period.hour_start, minute=period.minute_start))
                end = datetime.combine(for_date, time(hour=period.hour_end, minute=period.minute_end, second=59))
                data = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='15T'))
                data = data.assign(demand_rate=rate.rate, demand_period_name=period.period_name)
                rates = pd.concat([rates, data])

        rates['demand_rate'] = rates['demand_rate'].astype(float, errors='raise')
        return rates.sort_index()


@dataclass
class EnergyTierRate(ApiObject):
    _api_fields: ClassVar = (
        ApiField("id"),
        ApiField("energy_season_period_id"),
        ApiField("unit_start_value"),
        ApiField("unit_stop_value"),
        ApiField("unit"),
        ApiField("rate"),
        ApiField("adjustment"),
        ApiField("energySeasonPeriodId"),
    )

    id: int
    energy_season_period_id: int
    energySeasonPeriodId: int
    unit_start_value: int
    unit_stop_value: int
    unit: str
    rate: int
    adjustment: Optional[int] = None


@dataclass
class EnergySeasonPeriod(ApiObject):
    _api_fields: ClassVar = (
        ApiField("id", data_type=int),
        ApiField("energy_season_id", data_type=int),
        ApiField("day_of_week_start", data_type=int),
        ApiField("hour_start", data_type=int),
        ApiField("minute_start", data_type=int),
        ApiField("day_of_week_end", data_type=int),
        ApiField("hour_end", data_type=int),
        ApiField("minute_end", data_type=int),
        ApiField("period_name"),
        ApiField("energySeasonId"),
        ApiField("energy_tier_rates", data_type=EnergyTierRate),
    )

    id: int
    energy_season_id: int
    day_of_week_start: int
    hour_start: int
    minute_start: int
    day_of_week_end: int
    hour_end: int
    minute_end: int
    period_name: str
    energySeasonId: int
    energy_tier_rates: List[EnergyTierRate]


@dataclass
class EnergySeason(ApiObject):
    _api_fields: ClassVar = (
        ApiField("id"),
        ApiField("rate_schedule_id"),
        ApiField("season_name"),
        ApiField("season_type"),
        ApiField("start_month", data_type=int),
        ApiField("start_day", data_type=int),
        ApiField("end_month", data_type=int),
        ApiField("end_day", data_type=int),
        ApiField("energy_season_periods", data_type=EnergySeasonPeriod)
    )

    id: int
    rate_schedule_id: int
    season_name: str
    season_type: str
    start_month: int
    start_day: int
    end_month: int
    end_day: int
    energy_season_periods: List[EnergySeasonPeriod]

    def rates_for_date(self, for_date: date) -> pd.DataFrame:
        rates = pd.DataFrame()
        for period in self.energy_season_periods:
            if period.day_of_week_start <= for_date.isoweekday() - 1 <= period.day_of_week_end:
                if len(period.energy_tier_rates) > 1:
                    print('Warning -- multiple rates!')
                rate = period.energy_tier_rates[0]
                start = datetime.combine(for_date, time(hour=period.hour_start, minute=period.minute_start))
                end = datetime.combine(for_date, time(hour=period.hour_end, minute=period.minute_end, second=59))
                data = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='15T'))
                data = data.assign(usage_rate=rate.rate, usage_period_name=period.period_name)
                rates = pd.concat([rates, data])

        rates['usage_rate'] = rates['usage_rate'].astype(float, errors='raise')
        return rates.sort_index()


@dataclass
class RateSchedule(ApiObject):
    _api_fields: ClassVar = (
        ApiField("id"),
        ApiField("label", creatable=True),
        ApiField("description", creatable=True),
        ApiField("rate_schedule_type_id", creatable=True),
        ApiField("effective_start_date", data_type=Parsers.datetime, creatable=True),
        ApiField("effective_end_date", data_type=Parsers.datetime, creatable=True),
        ApiField("utility_provider_id", creatable=True),
        ApiField("sector", creatable=True),
        ApiField("source", creatable=True),
        ApiField("uri", creatable=True),
        ApiField("supersedes_rate_schedule_id", creatable=True),
        ApiField("openei_id", creatable=True),
        ApiField("is_rtp_rate", data_type=bool, creatable=True),
        ApiField("is_published", creatable=True),
        ApiField("facility_id", creatable=True),
        ApiField("utility_contract_id", creatable=True),
        ApiField("utility_provider", data_type=UtilityProvider),
        ApiField("rate_schedule_applicability", data_type=RateScheduleApplicability, optional=True),
        ApiField("rate_schedule_attributes", data_type=RateScheduleAttribute, optional=True),
        ApiField("rate_schedule_fixed_charges", data_type=RateScheduleFixedCharge, optional=True),
        ApiField("energy_seasons", data_type=EnergySeason, optional=True),
        ApiField("demand_seasons", data_type=DemandSeason, optional=True),
        ApiField("created_at", data_type=Parsers.datetime),
        ApiField("updated_at", data_type=Parsers.datetime),
    )

    id: int
    label: str
    description: str
    rate_schedule_type_id: int
    effective_start_date: datetime
    utility_provider_id: int
    sector: str
    is_rtp_rate: bool
    is_published: bool
    utility_provider: UtilityProvider
    rate_schedule_applicability: Optional[RateScheduleApplicability] = None
    rate_schedule_attributes: Optional[List[RateScheduleAttribute]] = field(default_factory=list)
    rate_schedule_fixed_charges: Optional[List[RateScheduleFixedCharge]] = field(default_factory=list)
    energy_seasons: List[EnergySeason] = field(default_factory=list)
    demand_seasons: List[DemandSeason] = field(default_factory=list)
    facility_id: Optional[int] = None
    utility_contract_id: Optional[int] = None
    openei_id: Optional[str] = None
    supersedes_rate_schedule_id: Optional[int] = None
    uri: Optional[str] = None
    source: Optional[str] = None
    effective_end_date: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def get_usage_dataframe_for_period(self, start_date: date, end_date: date) -> pd.DataFrame:

        if self.is_rtp_rate:
            raise RTPRateException(f'Cannot calculate usage for RTP Rate Schedule {self.label}')

        rates_for_range = pd.DataFrame()
        curr_date = start_date
        while True:
            for season in self.energy_seasons:
                if season.start_month <= curr_date.month <= season.end_month:
                    if season.start_day <= curr_date.day <= season.end_day:
                        rates_for_range = pd.concat([rates_for_range, season.rates_for_date(curr_date)])
            curr_date += timedelta(days=1)
            if curr_date > end_date:
                break

        return rates_for_range

    def get_demand_dataframe_for_period(self, start_date: date, end_date: date) -> (pd.DataFrame, pd.DataFrame):

        tou_rates_for_range = pd.DataFrame()
        flat_rates_for_range = pd.DataFrame()
        curr_date = start_date
        while True:
            for season in self.demand_seasons:
                if season.start_month <= curr_date.month <= season.end_month:
                    if season.start_day <= curr_date.day <= season.end_day:
                        if season.season_type == 'tou':
                            tou_rates_for_range = pd.concat([tou_rates_for_range, season.rates_for_date(curr_date)])
                        # TODO handle the "flat" side of these rates too
                        if season.season_type == 'flat':
                            flat_rates_for_range = pd.concat([flat_rates_for_range, season.rates_for_date(curr_date)])
            curr_date += timedelta(days=1)
            if curr_date > end_date:
                break

        return tou_rates_for_range, flat_rates_for_range