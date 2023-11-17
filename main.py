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
        val_size = row["val_size"]
        op = row["op"]
        ttl = row["ttl"]
        if tntid not in tenant_queries:
            tenant_queries[tntid] = []
        tenant_queries[tntid].append((ts, key, val_size, op, ttl))

    tenants: List[Tenant] = []
    for tntid in tenant_queries:
        queries = sorted(tenant_queries[tntid], key=lambda x: x[0])
        tss = [q[0] for q in queries]
        keys = [q[1] for q in queries]
        val_sizes = [q[2] for q in queries]
        ops = [q[3] for q in queries]
        ttls = [q[4] for q in queries]
        tnt = Tenant(tntid=tntid, time_series=tss, query_keys=keys,
                     val_sizes=val_sizes, ops=ops, ttls=ttls)
        tenants.append(tnt)
    return tenants


def main(tenants: List[Tenant], cache_scheme: CacheScheme, backingstore_scheme: BackingStoreScheme):
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
    df = None
    for tnt in tenants:
        tnt_df = tnt.dump_result()
        if df is None:
            df = tnt_df
        else:
            df = pd.concat([df, tnt_df], ignore_index=True)

    dst = f"results/{get_trace_name(trace_file)}_{svr.cache_client.allocator.name}_t{tnt.tntid}.csv"
    df.to_csv(dst, index=False)


def get_trace_name(trace_file: str) -> str:
    return os.path.splitext(os.path.basename(trace_file))[0]


def setup_cache_size(trace_df, cache_ratio) -> int:
    num_keys = len(set(trace_df["key"]))
    cache_size = int(num_keys * cache_ratio)
    print("cache size:", cache_size)
    return cache_size


if __name__ == "__main__":
    trace_file = "/home/yunchi/582/resalloc/data/memcached/dummy_q5000_d60_t3.csv"
    latency_mu = 1
    latency_sigma = 0
    cache_ratio = 1
    allocator_class = GlobalLRU

    trace_df = pd.read_csv(trace_file)
    tenants = parse_tenants(trace_df)

    cscheme = CacheScheme(
        cache_size=setup_cache_size(trace_df, cache_ratio),
        num_tenants=len(tenants),
        allocator_class=allocator_class)
    bscheme = BackingStoreScheme(
        latency_mu=latency_mu,
        latency_sigma=latency_sigma)
    main(tenants, cscheme, bscheme)
