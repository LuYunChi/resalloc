import time
import pandas as pd
from typing import List

from service.server import CacheServer


class Tenant:
    def __init__(self, tntid, time_series, query_keys) -> None:
        self.tntid = tntid
        self.time_series: List[float] = time_series
        self.query_keys: List[str] = query_keys

        self.log_issue: List[float] = []
        self.log_finish: List[float] = []
        self.log_hit: List[bool] = []
        assert len(time_series) == len(query_keys)

    def run(self, cache_svr: CacheServer, t0: float):
        last_finish = 0
        i = 0
        while i < len(self.time_series):
            t = time.time() - t0
            if t >= self.time_series[i] and t >= last_finish:
                hit, add_latency = cache_svr.request(self.tntid,
                                                     self.query_keys[i])
                last_finish = time.time() + add_latency

                self.log_issue.append(t)
                self.log_finish.append(last_finish)
                self.log_hit.append(hit)
                i += 1

    def dump_result(self, dst: str) -> None:
        data = {
            "original_ts": self.time_series,
            "issue_ts": self.log_issue,
            "finish_ts": self.log_finish,
            "hit": self.log_hit
        }
        pd.DataFrame(data).to_csv(data, index=False)
