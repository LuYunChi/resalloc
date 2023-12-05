import os
import time
import threading
import pandas as pd
from typing import List, Dict, Tuple

from tenant.tenant import Tenant
from service.server import CacheServer
from service.scheme import CacheScheme, BackingStoreScheme
from service.allocator.global_pooled_lru import GlobalPooledLRU
from service.allocator.maxmin import Maxmin
from service.allocator.amshare import AMShare
from service.allocator.memshare import Memshare


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


def main(tenants: List[Tenant], cache_scheme: CacheScheme, backingstore_scheme: BackingStoreScheme, dst: str):
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

    os.makedirs(os.path.dirname(dst), exist_ok=True)
    df.to_csv(dst, index=False)


def get_trace_name(trace_file: str) -> str:
    return os.path.splitext(os.path.basename(trace_file))[0]


def setup_cache_size(trace_df, cache_ratio) -> int:
    num_keys = len(trace_df[["key", "tntid"]].drop_duplicates())
    cache_size = int(num_keys * cache_ratio)
    print("cache size:", cache_size)
    return cache_size


if __name__ == "__main__":
    # trace_file = "/home/yunchi/582/resalloc/data/trace/selected_data_tenant10_time0-60_iter0.csv"
    # trace_file = "/home/yunchi/582/resalloc/data/trace/selected_data_tenant2_time0-10_iter0.csv"
    # trace_file = "data/trace/selected_data_tenant3_time0-900_iter0.csv"
    # trace_file = "data/trace/selected_data_tenant10_time0-900_iter0.csv"
    trace_file = "data/trace/selected_data_tenant2_time0-900_iter1.csv"
    # trace_file = "/home/yunchi/582/resalloc/data/trace/selected_data_tenant50_time0-900_iter0.csv"
    latency_sigma = 0
    for latency_mu in [3]:
        for cache_ratio in [0.5]:
            for allocator_class in [GlobalPooledLRU, Maxmin, AMShare, Memshare]:

                trace_df = pd.read_csv(trace_file)
                tenants = parse_tenants(trace_df)

                cscheme = CacheScheme(
                    cache_ratio=cache_ratio,
                    cache_size=setup_cache_size(trace_df, cache_ratio),
                    num_tenants=len(tenants),
                    allocator_class=allocator_class)
                bscheme = BackingStoreScheme(
                    latency_mu=latency_mu,
                    latency_sigma=latency_sigma)
                dst = f"results/lat{latency_mu}_cr{cache_ratio}/{get_trace_name(trace_file)}_{allocator_class.__name__}.csv"
                main(tenants, cscheme, bscheme, dst)
