from dataclasses import dataclass
from datetime import datetime as dt


@dataclass
class DownloadHistory:
    download_id: str | None
    resource_url: str | None
    check_sum: str | None
    entries: int | None
    download_at_utc: dt | None
