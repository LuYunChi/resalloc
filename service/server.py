import random
from typing import Tuple

from service.client import CacheClient
from service.scheme import CacheScheme, BackingStoreScheme
from service.allocator.abstract import Allocator


def create_allocator(cache_scheme: CacheScheme) -> Allocator:
    return Allocator(scheme=cache_scheme)


class CacheServer:
    def __init__(self, cache_scheme: CacheScheme, backingstore_scheme: BackingStoreScheme) -> None:
        self.cache_client = CacheClient(create_allocator(cache_scheme))
        self.backingstore_scheme = backingstore_scheme

        self.cache_client.reset()

    def request(self, tntid, key) -> Tuple[bool, float]:
        """ return true if direct hit, false if fetching from backing store """
        hit = self.cache_client.get(tntid, key)
        additional_latency = 0
        if not hit:
            additional_latency = self.fetch(key)
            self.cache_client.set(tntid, key)
        return hit, additional_latency

    def fetch(self, key) -> float:
        """ return simulated latency of fetching data from backing store """
        return max(0, random.gauss(self.backingstore_scheme.latency_mu,
                                   self.backingstore_scheme.latency_sigma))
