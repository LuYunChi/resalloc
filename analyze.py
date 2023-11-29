import pandas as pd
import matplotlib.pyplot as plt

WINDOW_SIZE = 5


def compare_tnts(df, dst):
    tnts = set(df["tntid"])

    fig, axs = plt.subplots(2, 3, figsize=(12, 6))

    # respective tps
    for t in tnts:
        d = df[df["tntid"] == t]
        x, y = get_tps(d)
        axs[0, 0].plot(x, y, label=t, marker='o')
    axs[0, 0].set_title('Respective Throughput')

    # respective lats
    for t in tnts:
        d = df[df["tntid"] == t]
        x, y = get_lats(d)
        axs[0, 1].plot(x, y, label=t, marker='o')
    axs[0, 1].set_title('Respective Latencies')

    # respective hrs
    for t in tnts:
        d = df[df["tntid"] == t]
        x, y = get_hrs(d)
        axs[0, 2].plot(x, y, label=t, marker='o')
    axs[0, 2].set_title('Respective Hit Rates')

    # overall tps
    x, y = get_tps(df)
    axs[1, 0].plot(x, y)
    axs[1, 0].set_title('Overall Throughput')

    # overall lats
    x, y = get_lats(df)
    axs[1, 1].plot(x, y)
    axs[1, 1].set_title('Overall Latencies')

    # overall hrs
    x, y = get_hrs(df)
    axs[1, 2].plot(x, y)
    axs[1, 2].set_title('Overall Hit Rates')

    # save
    plt.tight_layout()
    plt.savefig(dst)


def get_tps(df, ws=WINDOW_SIZE):
    max_t = df["finish_ts"].max()
    ts = [t for t in range(int(max_t)+2)]
    tps = []
    for t in ts:
        d = df[(t-ws < df["finish_ts"]) & (df["finish_ts"] <= t)]
        tp = d.count()/ws
        tps.append(tp)
    return ts, tps


def get_lats(df, ws=WINDOW_SIZE):
    max_t = df["finish_ts"].max()
    ts = [t for t in range(int(max_t)+2)]
    lats = []
    for t in ts:
        d = df[(t-ws < df["finish_ts"]) & (df["finish_ts"] <= t)]
        lat = (d["finish_ts"]-d["issue_ts"]).mean()
        lats.append(lat)
    return ts, lats


def get_hrs(df, ws=WINDOW_SIZE):
    max_t = df["finish_ts"].max()
    ts = [t for t in range(int(max_t)+2)]
    hrs = []
    for t in ts:
        d = df[(t-ws < df["finish_ts"]) & (df["finish_ts"] <= t)]
        h = d[d["hit"] == True].count()/ws
        hrs.append(h)
    return ts, hrs


if __name__ == "__main__":
    path = "/home/yunchi/582/resalloc/results/selected_data_tenant3_time0-60_iter0_GlobalLRU.csv"

    df = pd.read_csv(path)
    compare_tnts(df, path.replace(".csv", ".png"))
