from enum import Enum
import time
from typing import Tuple, List, Dict, Optional
from collections import defaultdict
from dataclasses import dataclass

from service.scheme import CacheScheme
from service.allocator.abstract import Allocator


class EntryStatus(Enum):
    IN_SMALL = 1
    IN_MAIN_DEMOTABLE = 2
    IN_MAIN_FULL = 3


@dataclass
class CacheObject:
    tntid: str
    key: str
    last_used: float
    status: EntryStatus
    use_cnt: int


class AMShare(Allocator):
    def __init__(self, scheme: CacheScheme) -> None:
        self.name = "AMShare"
        self.scheme = scheme

        self.small_qs: Dict[str, List[CacheObject]] = defaultdict(list)
        self.victim_qs: Dict[str, List[str]] = defaultdict(list)
        self.main_cache: List[CacheObject] = []

    @property
    def sq_size(self):
        return int(self.scheme.cache_size
                   / self.scheme.num_tenants
                   * self.scheme.guarantee_ratio
                   * self.scheme.smallq_size_ratio)

    @property
    def guar_size(self):
        return int(self.scheme.cache_size
                   / self.scheme.num_tenants
                   * self.scheme.guarantee_ratio)

    def _find_in_sq(self, tntid, key) -> Optional[CacheObject]:
        for co in self.small_qs[tntid]:
            if co.key == key:
                return co
        return None

    def _find_in_vq(self, tntid, key) -> bool:
        for k in self.victim_qs[tntid]:
            if k == key:
                return True
        return False

    def _find_in_maincache(self, tntid, key) -> Optional[CacheObject]:
        for co in self.main_cache:
            if co.tntid == tntid and co.key == key:
                return co
        return None

    def _find_co(self, tntid, key) -> Optional[CacheObject]:
        co = self._find_in_sq(tntid, key)
        if co is not None:
            return co
        co = self._find_in_maincache(tntid, key)
        if co is not None:
            return co
        return None

    def key_in_cache(self, tntid, key) -> bool:
        """ return whether key is in cache """
        return self._find_co(tntid, key) is not None

    def _cache_cnt(self) -> int:
        sq_cnt = 0
        for _, sq in self.small_qs.items():
            sq_cnt += len(sq)
        return len(self.main_cache) + sq_cnt

    def cache_isfull(self) -> bool:
        """ return whether cache is full """
        return self._cache_cnt() >= self.scheme.cache_size

    def inform_use(self, tntid, key) -> None:
        """ called when a key is read or updated; only update last use ts """
        co = self._find_co(tntid, key)
        co.last_used = time.time()
        if co.status == EntryStatus.IN_MAIN_FULL:
            pass
        elif co.status == EntryStatus.IN_MAIN_DEMOTABLE:
            co.status = EntryStatus.IN_MAIN_FULL
        elif co.status == EntryStatus.IN_SMALL:
            co.use_cnt += 1
        else:
            raise

    def inform_set(self, tntid, key, ttl) -> None:
        """ called when a new key is brought into cache """
        # cc0 = self._cache_cnt()
        key_in_victim = self._find_in_vq(tntid, key)
        if key_in_victim:
            co = CacheObject(tntid, key, time.time(),
                             EntryStatus.IN_MAIN_FULL, 1)
            self.main_cache.append(co)
        else:
            co = CacheObject(tntid, key, time.time(), EntryStatus.IN_SMALL, 1)
            self._append_sq(co)
        # assert self._cache_cnt() == cc0 + 1

    def _append_sq(self, co: CacheObject) -> None:
        # cc0 = self._cache_cnt()
        tntid = co.tntid
        self.small_qs[tntid].append(co)
        if len(self.small_qs[tntid]) > self.sq_size:
            pop = self.small_qs[tntid][0]
            self.small_qs[tntid] = self.small_qs[tntid][1:]
            pop.status = EntryStatus.IN_MAIN_FULL if pop.use_cnt > 1 else EntryStatus.IN_MAIN_DEMOTABLE
            self.main_cache.append(pop)
        # assert self._cache_cnt() == cc0 + 1

    def _append_vq(self, tntid, key) -> None:
        self.victim_qs[tntid].append(key)
        # victim q has same len as guaranteed
        if len(self.victim_qs[tntid]) > self.guar_size:
            self.victim_qs[tntid] = self.victim_qs[tntid][1:]

    def arbit_evict(self, tntid, key) -> Tuple[str, str]:
        """ called when a new key should be brought in and cache is full """
        # cc0 = self._cache_cnt()
        # get victim tenant candidates
        tenant_cachecnts: Dict[str, int] = defaultdict(int)
        for t, q in self.small_qs.items():
            tenant_cachecnts[t] += len(q)
        for co in self.main_cache:
            tenant_cachecnts[co.tntid] += 1
        victen_cands = [t for t, cnt in tenant_cachecnts.items()
                        if cnt > self.guar_size]
        # try to evict from demotables in main cache
        demotable = []
        for i, co in enumerate(self.main_cache):
            if co.status == EntryStatus.IN_MAIN_DEMOTABLE and co.tntid in victen_cands:
                demotable.append((co.last_used, i, co.tntid, co.key))
        if demotable:
            _, i, etntid, ekey = sorted(demotable)[0]
            self._append_vq(etntid, ekey)
            self.main_cache = self.main_cache[:i] + self.main_cache[i+1:]
            # assert self._cache_cnt() == cc0 - 1
            return etntid, ekey
        # to reach here, all entries in main cache are frequently accessed
        # first, look at smallq of the inserting tenant
        if tntid in victen_cands:
            assert self.small_qs[tntid], f"{self.small_qs}, {tenant_cachecnts}, {victen_cands}"
            head = self.small_qs[tntid][0]
            if head.use_cnt == 1:
                self.small_qs[tntid] = self.small_qs[tntid][1:]
                self._append_vq(head.tntid, head.key)
                # assert self._cache_cnt() == cc0 - 1
                return head.tntid, head.key
        # then, evict from IN_MAIN_FULL
        evictable = []
        for i, co in enumerate(self.main_cache):
            if co.tntid in victen_cands:
                evictable.append((co.last_used, i, co.tntid, co.key))
        _, i, etntid, ekey = sorted(evictable)[0]
        self._append_vq(etntid, ekey)
        self.main_cache = self.main_cache[:i] + self.main_cache[i+1:]
        # assert self._cache_cnt() == cc0 - 1
        return etntid, ekey
