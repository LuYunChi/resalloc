

import random
from typing import List, Optional, Tuple
import matplotlib.pyplot as plt


class Entry:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.valid = False
        self.last_used = -1
        self.key = 0
        self.clientname = ""


class Cache:
    def __init__(self, size) -> None:
        self.cache = [Entry() for _ in range(size)]

    def page_in(self, time, entry_id, clientname, key) -> None:
        assert self.cache[entry_id].valid == False
        self.cache[entry_id].valid = True
        self.cache[entry_id].last_used = time
        self.cache[entry_id].key = key
        self.cache[entry_id].clientname = clientname

    def page_out(self, entry_id) -> None:
        assert self.cache[entry_id].valid == True
        self.cache[entry_id].reset()

    def get_entry_id(self, clientname, key) -> Optional[int]:
        for i, e in enumerate(self.cache):
            if e.clientname == clientname and e.key == key:
                return i
        return None

    def use(self, entry_id, time) -> None:
        self.cache[entry_id].last_used = time

    def get_unused_or_lru(self, start, end) -> Tuple[int, bool]:
        # return entry_id, used
        lru_time = None
        lru_ptr = None
        for i in range(start, end+1):
            e = self.cache[i]
            if lru_time is None or lru_time > e.last_used:
                lru_time = e.last_used
                lru_ptr = i
            if e.valid == False:
                return i, False
        return lru_ptr, True


class FullShareCache(Cache):
    def __init__(self, size) -> None:
        super().__init__(size)
        self.name = "FullShareCache"

    def query(self, time, clientname, key) -> bool:
        # return True if hit else False
        eid = self.get_entry_id(clientname, key)
        if eid is not None:  # cache hit
            self.use(eid, time)
        else:  # cache miss
            eid_new, used = self.get_unused_or_lru(0, len(self.cache)-1)
            if used:
                self.page_out(eid_new)
            self.page_in(time, eid_new, clientname, key)
        return eid is not None


class EvenSplitCache(Cache):
    def __init__(self, size, clientnames) -> None:
        super().__init__(size)
        self.name = "EvenSplitCache"

        def get_range():
            share, remain = divmod(size, len(clientnames))
            d = {}
            s = 0
            for i, u in enumerate(clientnames):
                r = 1 if i < remain else 0
                e = s+share+r-1
                d[u] = (s, e)
                s = e+1
            return d
        self.range = get_range()    # clientname -> allocated entries
        # clientname -> number of used entries
        self.use_cnt = {u: 0 for u in clientnames}

    def page_in(self, time, entry_id, clientname, key) -> None:
        super().page_in(time, entry_id, clientname, key)
        self.use_cnt[clientname] += 1

    def page_out(self, entry_id) -> None:
        clientname = self.cache[entry_id].clientname
        self.use_cnt[clientname] -= 1
        super().page_out(entry_id)

    def query(self, time, clientname, key) -> bool:
        # return True if hit else False
        eid = self.get_entry_id(clientname, key)
        if eid is not None:  # cache hit
            self.use(eid, time)
        else:  # cache miss
            rg = self.range[clientname]
            eid_new, used = self.get_unused_or_lru(rg[0], rg[1])
            if used:
                self.page_out(eid_new)
            self.page_in(time, eid_new, clientname, key)
        return eid is not None


class Client:
    def __init__(self, name, max_query_key, query_interval) -> None:
        self.name = name
        self.q_max_key = max_query_key
        self.q_itv = query_interval

    def should_query(self, time) -> bool:
        return time % self.q_itv == 0

    def gen_key(self) -> int:
        return random.randint(1, self.q_max_key)


HIT_TIME = 1
MISS_TIME = 10


class Log:
    def __init__(self) -> None:
        self.log = {}
        self.clientnames = set()
        self.cachenames = set()
        self.maxtime = 0

    def add(self, time, clientname, hit, cachename):
        self.clientnames.add(clientname)
        self.cachenames.add(cachename)
        self.maxtime = max(self.maxtime, time)

        if clientname not in self.log:
            self.log[clientname] = {}
        if cachename not in self.log[clientname]:
            self.log[clientname][cachename] = []
        self.log[clientname][cachename].append((time, hit))

    def get_avg_access_time_trace(self, clientname, cachename) -> List[float]:
        hits = [0] * (self.maxtime+1)
        qrys = [0] * (self.maxtime+1)
        for t, h in self.log[clientname][cachename]:
            qrys[t] = 1
            hits[t] = 1 if h else 0

        num_hits = 0
        num_qrys = 0
        trace = []
        for t in range(self.maxtime+1):
            num_hits += hits[t]
            num_qrys += qrys[t]
            num_misses = num_qrys - num_hits
            avg = (num_hits * HIT_TIME + num_misses * MISS_TIME) / num_qrys
            trace.append(avg)
        return trace

    def draw_avg_access_time(self) -> None:
        num_plots = len(self.cachenames)
        x_axis = [i for i in range(self.maxtime+1)]

        _, axes = plt.subplots(1, num_plots, figsize=(
            10, 3), sharex=True, sharey=True)

        # Plot each graph in a separate subplot
        for graph_id, cachename in enumerate(self.cachenames):
            ax = axes[graph_id]
            for clientname in self.clientnames:
                data = self.get_avg_access_time_trace(clientname, cachename)
                assert len(data) == len(x_axis)
                ax.plot(x_axis, data, label=clientname)
            ax.set_title(cachename)
            ax.set_xlabel('iterations')
            ax.set_ylabel('Avg access time')
            ax.legend()
        plt.ylim(bottom=0)
        plt.show()


def main(cache_size, clients: List[Client], iterations):
    clientnames = [u.name for u in clients]
    even_split_cache = EvenSplitCache(cache_size, clientnames)
    full_share_cache = FullShareCache(cache_size)

    log = Log()
    for time in range(iterations):
        for client in clients:
            if client.should_query(time):
                key = client.gen_key()
                for cache in [even_split_cache, full_share_cache]:
                    hit = cache.query(time, client.name, key)
                    log.add(time, client.name, hit, cache.name)
    log.draw_avg_access_time()


if __name__ == "__main__":
    cache_size = 100
    clients = {
        Client("u1", 70, 2),
        Client("u2", 70, 4),
        # Client("u3", 500, 2),
    }
    iterations = 1000
    main(cache_size, clients, iterations)
