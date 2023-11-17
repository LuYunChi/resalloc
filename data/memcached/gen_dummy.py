import random
import pandas as pd

sample_trace = "memcached-sample"
max_numq = 5000
time_span = 60
num_tenants = 3


df = pd.read_csv(sample_trace, nrows=max_numq,
                 names=["ts", "key", "key_size", "val_size", "client_id", "op", "ttl"])
df.drop("key_size", axis=1, inplace=True)
df.sort_values(by="client_id", inplace=True)
df.reset_index(inplace=True, drop=True)

bin_edges = []
bin_size = len(df) // num_tenants
for i in range(num_tenants):
    ii = min(len(df), (i+1)*bin_size)-1
    bin_edges.append((df.loc[ii, "client_id"], i))

for i in range(len(df)):
    new_client_id = 0
    for bin_edge, cid in bin_edges:
        if df.loc[i, "client_id"] <= bin_edge:
            new_client_id = cid
            break
    df.loc[i, "client_id"] = new_client_id
    df.loc[i, "ts"] = int(random.uniform(0, time_span))
    df.loc[i, "op"] = "get" if df.loc[i, "op"] in ["get", "gets"] else "set"

df.rename(columns={'client_id': 'tntid'}, inplace=True)
df.sort_values(by="ts", inplace=True)
df.reset_index(inplace=True, drop=True)

df.to_csv(f"dummy_q{len(df)}_d{time_span}_t{num_tenants}.csv", index=False)
