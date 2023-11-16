import os
import time
import threading
import pandas as pd
from typing import List, Dict, Tuple

from tenant.tenant import Tenant
from service.server import CacheServer
from service.scheme import CacheScheme, BackingStoreScheme
from service.allocator.lru import GlobalLRU


def parse_tenants(df) -> List[Tenant]:
    tenant_queries: Dict[List[Tuple[float, str]]] = {}
    for _, row in df.iterrows():
        ts = row["ts"]
        tntid = row["tntid"]
        key = row["key"]
        if tntid not in tenant_queries:
            tenant_queries[tntid] = []
        tenant_queries[tntid].append((ts, key))

    tenants: List[Tenant] = []
    for tntid in tenant_queries:
        queries = sorted(tenant_queries[tntid], key=lambda x: x[0])
        tss = [q[0] for q in queries]
        keys = [q[1] for q in queries]
        tnt = Tenant(tntid=tntid, time_series=tss, query_keys=keys)
        tenants.append(tnt)
    return tenants


def main(trace_file: str, cache_scheme: CacheScheme, backingstore_scheme: BackingStoreScheme):
    df = pd.read_csv(trace_file)
    tenants: List[Tenant] = parse_tenants(df)
    svr = CacheServer(cache_scheme=cache_scheme,
                      backingstore_scheme=backingstore_scheme)
    threads = []
    t0 = time.time()
    for tnt in tenants:
        t = threading.Thread(target=tnt.run, args=[svr, t0])
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    for tnt in tenants:
        dst = f"{get_trace_name(trace_file)}_{svr.cache_client.allocator.name}_t{tnt.tntid}.json"
        tnt.dump_result(dst)


def get_trace_name(trace_file: str) -> str:
    return os.path.splitext(os.path.basename(trace_file))[0]


if __name__ == "__main__":
    trace_file = "/home/yunchi/582/resalloc/data/memcached/dummy_q100_d20_t4.csv"
    num_tenants = 4
    cache_size = 100
    latency_mu = 1
    latency_sigma = 1

    cscheme = CacheScheme(
        cache_size=cache_size,
        num_tenants=num_tenants,
        allocator_class=GlobalLRU)
    bscheme = BackingStoreScheme(
        latency_mu=latency_mu,
        latency_sigma=latency_sigma)
    main(trace_file, cscheme, bscheme)
