import time
import json
import pandas as pd
from typing import List

from service.server import CacheServer


class Tenant:
    def __init__(self, tntid, time_series, query_keys, val_sizes, ops, ttls) -> None:
        self.tntid = tntid
        self.time_series: List[float] = time_series
        self.query_keys: List[str] = query_keys
        self.val_sizes: List[int] = val_sizes
        self.ops: List[str] = ops
        self.ttls: List[int] = ttls

        self.log_issue: List[float] = []
        self.log_finish: List[float] = []
        self.log_hit: List[bool] = []
        assert len(time_series) == len(query_keys)

    def run(self, cache_svr: CacheServer, t0: float):
        last_finish = 0
        i = 0
        while i < len(self.time_series):
            t = time.time() - t0
            # if t >= self.time_series[i] and t >= last_finish:
            if t >= self.time_series[i]:
                print(f"tenant {self.tntid}: {i+1}/{len(self.query_keys)}")
                hit, add_latency = cache_svr.request(tntid=self.tntid,
                                                     key=self.query_keys[i],
                                                     iswrite=self.ops[i] != "get",
                                                     val_size=self.val_sizes[i],
                                                     ttl=self.ttls[i]
                                                     )
                last_finish = time.time() - t0 + add_latency

                self.log_issue.append(t)
                self.log_finish.append(last_finish)
                self.log_hit.append(hit)
                i += 1

    def dump_result(self):
        data = {
            "tntid": [self.tntid] * len(self.time_series),
            "original_ts": self.time_series,
            "issue_ts": self.log_issue,
            "finish_ts": self.log_finish,
            "hit": self.log_hit,
        }
        return pd.DataFrame(data)
