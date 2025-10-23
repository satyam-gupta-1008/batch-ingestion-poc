from typing import Any, Dict, Iterator, Mapping, Optional
from datetime import datetime, timedelta
from airbyte_cdk.sources.declarative.partition_routers import PartitionRouter

class Partition(dict):
    def __init__(self, partition: dict, extra_fields: Optional[dict] = None):
        super().__init__(partition)
        self.partition = partition
        self.extra_fields = extra_fields or {}

    def __repr__(self):
        return dict(self).__repr__()

class DateRangePartitionRouter(PartitionRouter):
    parameters: Mapping[str, Any]
    config: Mapping[str, Any]
    def __init__(self, parameters: Mapping[str, Any], config: Mapping[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.start_date = config.get("start_date")
        self.end_date = config.get("end_date")
        self.step_days = int(parameters.get("step_days"))
        self._state: Optional[dict] = None

    def stream_slices(self) -> Iterator[Mapping[str, Any]]:
        start_dt = datetime.strptime(self.start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(self.end_date, "%Y-%m-%d")

        current = start_dt
        while current <= end_dt:
            next_end = min(current + timedelta(days=self.step_days - 1), end_dt)
            yield Partition({
                "start_date": current.strftime("%Y-%m-%d"),
                "end_date": next_end.strftime("%Y-%m-%d"),
            })
            current = next_end + timedelta(days=1)

    def get_stream_state(self) -> Optional[dict]:
        return self._state

    def set_initial_state(self, state: Optional[dict]) -> None:
        self._state = state

    def get_request_headers(self, stream_slice: Optional[dict] = None, stream_state: Optional[dict] = None, **kwargs) -> dict:
        return {}
    
    def get_request_params(self, stream_slice: Optional[dict] = None, stream_state: Optional[dict] = None, **kwargs) -> dict:
        return {}

    def get_request_body_json(self, stream_slice: Optional[dict] = None, stream_state: Optional[dict] = None, **kwargs) -> dict:
        return {}
    
    def get_request_body_data(self, stream_slice: Optional[dict] = None, stream_state: Optional[dict] = None, **kwargs) -> dict:
        return {}

