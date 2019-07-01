from datetime import datetime
from typing import Dict, List, Optional

from contxt.auth import Auth
from contxt.models.iot import Feed, Field, FieldGrouping, UnprovisionedField, Window
from contxt.services.api import ApiEnvironment, ConfiguredApi


class IotService(ConfiguredApi):
    """
    Service to interact with our IOT API.

    Terminology
        - Feed: Data source (i.e. utility meter) with a set of fields
        - Field: A specific time series data from a feed
        - Grouping: Group of fields
    """

    _envs = (
        ApiEnvironment(
            name="production",
            base_url="https://feeds.api.ndustrial.io/v1",
            client_id="iznTb30Sfp2Jpaf398I5DN6MyPuDCftA",
        ),
    )

    def __init__(self, auth: Auth, env: str = "production"):
        super().__init__(env, auth)

    def get_feed_with_id(self, id: int) -> Feed:
        return Feed.from_api(self.get(f"feeds/{id}"))

    def get_feed_with_key(self, key: str) -> Optional[Feed]:
        feeds = self.get_feeds(key=key)
        if len(feeds) == 0:
            return None
        elif len(feeds) == 1:
            return feeds[0]
        raise KeyError(f"Expected singleton feed with key {key}, not {len(feeds)}")

    def get_feeds(
        self, facility_id: Optional[int] = None, key: Optional[str] = None
    ) -> List[Feed]:
        return [
            Feed.from_api(rec)
            for rec in self.get(
                "feeds", params={"facility_id": facility_id, "key": key}
            )
        ]

    def get_fields_for_facility(self, facility_id: int) -> List[Field]:
        return [
            Field.from_api(rec) for rec in self.get(f"facilities/{facility_id}/fields")
        ]

    def get_fields_for_feed(
        self, feed_id: int, limit: int = 1000, offset: int = 0
    ) -> List[Field]:
        return [
            Field.from_api(rec)
            for rec in self.get(
                f"feeds/{feed_id}/fields", params={"limit": limit, "offset": offset}
            )
        ]

    def get_field_data(
        self,
        field: Field,
        start_time: datetime,
        window: Window = Window.RAW,
        end_time: Optional[datetime] = None,
        limit: int = 1000,
    ) -> Dict:
        # Manually validate the window choice, since our API does not return a
        # helpful error message
        assert isinstance(window, Window), "window must be of type Window"
        resp = self.get(
            f"outputs/{field.output_id}/fields/{field.field_human_name}/data",
            params={
                "timeStart": int(start_time.timestamp()),
                "timeEnd": int(end_time.timestamp()) if end_time else None,
                "window": window.value,
                "limit": limit,
            },
        )
        # NOTE: this intentionally does not yet handle pagination, or parsing
        # (coming soon)
        return resp

    def get_field_data_for_grouping(self, grouping_id: str, **kwargs) -> List[Field]:
        grouping = self.get_grouping(grouping_id)
        return [self.get_field_data(field=f, **kwargs) for f in grouping.fields]

    def get_unprovisioned_fields_for_feed_id(
        self, feed_id: int
    ) -> List[UnprovisionedField]:
        return [
            UnprovisionedField.from_api(rec)
            for rec in self.get(f"feeds/{feed_id}/fields/unprovisioned")
        ]

    def get_unprovisioned_fields_for_feed_key(
        self, feed_key: str
    ) -> Optional[List[UnprovisionedField]]:
        feed = self.get_feed_with_key(key=feed_key)
        if not feed:
            return None
        return [
            UnprovisionedField.from_api(rec)
            for rec in self.get(f"feeds/{feed.id}/fields/unprovisioned")
        ]

    def get_field_grouping(self, id: str) -> FieldGrouping:
        return FieldGrouping.from_api(self.get(f"groupings/{id}"))

    def get_field_groupings_for_facility(self, facility_id: int) -> List[FieldGrouping]:
        return [
            FieldGrouping.from_api(rec)
            for rec in self.get(f"facilities/{facility_id}/groupings")
        ]
