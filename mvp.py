

import random
from typing import List, Optional, Tuple
import matplotlib.pyplot as plt
from copy import deepcopy


class Entry:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.valid = False
        self.last_used = -1
        self.key = 0
        self.clientname = ""


class BaseCache:
    def __init__(self) -> None:
        self.name = "BaseCache"
        self.cache = []

    def register(self, *args, **kwargs) -> None:
        raise NotImplementedError

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
        """ return entry_id, used """
        lru_time = None
        lru_ptr = None
        for i, e in enumerate(self.cache[start: end+1]):
            if lru_time is None or lru_time > e.last_used:
                lru_time = e.last_used
                lru_ptr = i + start
            if e.valid == False:
                return i + start, False
        return lru_ptr, True

    def query(self, time, clientname, key) -> bool:
        """ return True if hit else False """
        eid = self.get_entry_id(clientname, key)
        hit = eid is not None
        if hit:  # cache hit
            self.use(eid, time)
        else:  # cache miss
            eid_new, used = self.schedule_pagein_eid(time, clientname)
            if used:
                self.page_out(eid_new)
            self.page_in(time, eid_new, clientname, key)
        return hit

    def schedule_pagein_eid(self, time, clientname) -> Tuple[int, bool]:
        # return entry_id, used
        raise NotImplementedError


class FullShareCache(BaseCache):
    def __init__(self) -> None:
        super().__init__()
        self.name = "FullShareCache"

    def register(self, size, *args, **kwargs) -> None:
        self.cache = [Entry() for _ in range(size)]

    def schedule_pagein_eid(self, time, clientname) -> Tuple[int, bool]:
        """ return entry_id, used """
        return self.get_unused_or_lru(0, len(self.cache)-1)


class EvenSplitCache(BaseCache):
    def __init__(self) -> None:
        super().__init__()
        self.name = "EvenSplitCache"

        self.clientnames = []
        self.use_cnt = {}  # clientname -> number of used entries
        self.range = {}  # clientname -> allocated entries

    def register(self, size, clientnames, *args, **kwargs) -> None:
        self.cache = [Entry() for _ in range(size)]
        self.clientnames = clientnames
        self.use_cnt = {u: 0 for u in clientnames}

        share, remain = divmod(size, len(clientnames))
        s = 0
        for i, u in enumerate(clientnames):
            r = 1 if i < remain else 0
            e = s + share + r - 1
            self.range[u] = (s, e)
            s = e + 1

    def page_in(self, time, entry_id, clientname, key) -> None:
        self.use_cnt[clientname] += 1
        super().page_in(time, entry_id, clientname, key)

    def page_out(self, entry_id) -> None:
        self.use_cnt[self.cache[entry_id].clientname] -= 1
        super().page_out(entry_id)

    def schedule_pagein_eid(self, time, clientname) -> Tuple[int, bool]:
        """ return entry_id, used """
        rg = self.range[clientname]
        return self.get_unused_or_lru(rg[0], rg[1])


class MaxMinFairCache(BaseCache):
    def __init__(self) -> None:
        super().__init__()
        self.name = "MaxMinFairCache"

        self.clientnames = []
        self.use_cnt = {}  # clientname -> number of used entries

    def register(self, size, clientnames, *args, **kwargs) -> None:
        self.cache = [Entry() for _ in range(size)]
        self.clientnames = clientnames
        self.use_cnt = {u: 0 for u in clientnames}

    def page_in(self, time, entry_id, clientname, key) -> None:
        self.use_cnt[clientname] += 1
        super().page_in(time, entry_id, clientname, key)

    def page_out(self, entry_id) -> None:
        self.use_cnt[self.cache[entry_id].clientname] -= 1
        super().page_out(entry_id)

    def schedule_pagein_eid(self, time, clientname) -> Tuple[int, bool]:
        """ return entry_id, used """
        clientname_victim = sorted(
            self.use_cnt.items(), key=lambda x: x[1], reverse=True)[0][0]
        lru_time = None
        pagein_eid = None
        for i, e in enumerate(self.cache):
            if e.valid == False:  # has unused cache
                return i, False
            elif e.clientname == clientname_victim:  # cache used by victim
                if lru_time is None or lru_time > e.last_used:
                    lru_time = e.last_used
                    pagein_eid = i
        assert pagein_eid is not None
        return pagein_eid, True


class HitRateFairCache(BaseCache):
    def __init__(self, fairness_ttl) -> None:
        super().__init__()
        self.name = f"HitRateFairCache-fttl={fairness_ttl}"
        self.fttl = fairness_ttl

        self.clientnames = []
        self.history = {}  # clientname -> [(time, hit)]

    def register(self, size, clientnames, *args, **kwargs) -> None:
        self.cache = [Entry() for _ in range(size)]
        self.clientnames = clientnames
        self.history = {c: [] for c in clientnames}

    def add_history(self, time, clientname, hit) -> None:
        self.history[clientname].append((time, hit))

    def prune_history(self, time, clientname) -> None:
        i = 0
        while i < len(self.history[clientname]) and self.history[clientname][i][0] < time-self.fttl:
            i += 1
        self.history[clientname] = self.history[clientname][i:]

    def schedule_pagein_eid(self, time, clientname) -> Tuple[int, bool]:
        """ return entry_id, used """
        def recent_hit_rate(cname):
            self.prune_history(time, cname)
            history = self.history[cname]
            nhit = len([1 for _, hit in history if hit])
            return nhit / max(1, len(history))
        hit_rates = [(recent_hit_rate(c), c) for c in self.clientnames]
        # choose client with highest recent hit rate to be the victim
        clientname_victims = [c for _, c in sorted(hit_rates, reverse=True)]
        lru_time = None
        pagein_eid = None
        for clientname_victim in clientname_victims:
            for i, e in enumerate(self.cache):
                if e.valid == False:  # has unused cache
                    return i, False
                elif e.clientname == clientname_victim:  # cache used by victim
                    if lru_time is None or lru_time > e.last_used:
                        lru_time = e.last_used
                        pagein_eid = i
            if pagein_eid is not None:
                break
        assert pagein_eid is not None, hit_rates
        return pagein_eid, True

    def query(self, time, clientname, key) -> bool:
        """ return True if hit else False """
        hit = super().query(time, clientname, key)
        self.add_history(time, clientname, hit)
        return hit


class Client:
    def __init__(self, name, max_query_key, query_interval, stddev=None) -> None:
        self.name = name
        self.q_max_key = max_query_key
        self.q_itv = query_interval
        self.stddev = stddev

    def should_query(self, time) -> bool:
        return time % self.q_itv == 0

    def gen_key(self) -> int:
        if self.stddev is None:  # use uniform random
            return random.randint(1, self.q_max_key)
        else:  # use gussian random
            r = int(random.gauss(self.q_max_key / 2, self.stddev))
            r = max(1, min(self.q_max_key, r))
            return r


HIT_TIME = 1
MISS_TIME = 10


class Log:
    def __init__(self) -> None:
        self.log = {}
        self.clientnames = []
        self.cachenames = []
        self.maxtime = 0

    def add(self, time, clientname, hit, cachename):
        if clientname not in self.clientnames:
            self.clientnames.append(clientname)
        if cachename not in self.cachenames:
            self.cachenames.append(cachename)
        self.maxtime = max(self.maxtime, time)

        if clientname not in self.log:
            self.log[clientname] = {}
        if cachename not in self.log[clientname]:
            self.log[clientname][cachename] = []
        self.log[clientname][cachename].append((time, hit))

    def get_full_avg_acctime_trace(self, clientname, cachename) -> List[float]:
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
            4*num_plots, 3), sharex=True, sharey=True)

        # Plot each graph in a separate subplot
        for graph_id, cachename in enumerate(self.cachenames):
            ax = axes[graph_id]
            for clientname in sorted(self.clientnames):
                data = self.get_full_avg_acctime_trace(clientname, cachename)
                assert len(data) == len(x_axis)
                ax.plot(x_axis, data, label=clientname)
            ax.set_title(cachename)
            ax.set_xlabel('iterations')
            ax.set_ylabel('Avg access time')
            ax.legend()
        plt.ylim(bottom=0)
        plt.show()


def main(caches: List[BaseCache], cache_size: int,  clients: List[Client], iterations: int):
    caches = [deepcopy(c) for c in caches]  # enusre experiment isolation
    clientnames = [u.name for u in clients]
    for cache in caches:
        cache.register(
            size=cache_size,
            clientnames=clientnames)

    log = Log()
    for time in range(iterations):
        for client in clients:
            if client.should_query(time):
                key = client.gen_key()
                for cache in caches:
                    hit = cache.query(time, client.name, key)
                    log.add(time, client.name, hit, cache.name)
    log.draw_avg_access_time()


if __name__ == "__main__":
    iterations = 1000
    caches = [
        FullShareCache(),
        EvenSplitCache(),
        MaxMinFairCache(),
        HitRateFairCache(fairness_ttl=20),
        # HitRateFairCache(fairness_ttl=100),
    ]
    cache_size = 100
    clients = {
        Client("u1", 50, 1),
        Client("u2", 50, 1),
        Client("u3", 50, 1),
    }
    clients = {
        Client("u1", 50, 1),
        Client("u2", 50, 2),
        Client("u3", 50, 5),
    }
    clients = {
        Client("u1", 20, 1),
        Client("u2", 30, 1),
        Client("u3", 100, 1),
    }
    clients = {
        Client("u1", 20, 1),
        Client("u2", 30, 2),
        Client("u3", 100, 5),
    }
    clients = {
        Client("u1", 20, 5),
        Client("u2", 30, 2),
        Client("u3", 100, 1),
    }
    # clients = {
    #     Client("u1", 40, 1),
    #     Client("u2", 80, 1),
    #     Client("u3", 200, 1),
    # }
    # clients = {
    #     Client("u1", 40, 1),
    #     Client("u2", 80, 2),
    #     Client("u3", 200, 5),
    # }
    # clients = {
    #     Client("u1", 100, 1, stddev=10),
    #     Client("u2", 100, 1, stddev=80),
    # }
    # clients = {
    #     Client("u1", 200, 1, stddev=10),
    #     Client("u2", 200, 1, stddev=80),
    # }
    # clients = {
    #     Client("u1", 40, 1),
    #     Client("u2", 1000, 1),
    # }
    # clients = {
    #     Client("u1", 100, 2),
    #     Client("u2", 100, 1),
    # }
    main(caches, cache_size, clients, iterations)
