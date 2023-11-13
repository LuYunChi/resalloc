import pandas as pd

TS = "ts"
KEY = "key"
KEY_SIZE = "key_size"
VAL_SIZE = "val_size"
CLIENT_ID = "client_id"
OP = "op"
TTL = "ttl"

df = pd.read_csv("cluster01.000", skiprows=10000000, nrows=10000000,
                 names=[TS, KEY, KEY_SIZE, VAL_SIZE, CLIENT_ID, OP, TTL])
# ns = set()
# for k in df[KEY]:
#     tokens = k.split(":")
#     ns.add(tokens[0])
# print(len(ns))
# print(len(df[df[CLIENT_ID] == df[CLIENT_ID][0]]))
print(df[TS].min(), df[TS].max())
print(len(set(df[CLIENT_ID])))
