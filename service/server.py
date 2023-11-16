import os
import random
import secrets
from typing import Tuple

from service.client import CacheClient
from service.scheme import CacheScheme, BackingStoreScheme
from service.allocator.abstract import Allocator


def create_allocator(cache_scheme: CacheScheme) -> Allocator:
    return cache_scheme.allocator_class(scheme=cache_scheme)


class CacheServer:
    def __init__(self, cache_scheme: CacheScheme, backingstore_scheme: BackingStoreScheme) -> None:
        self.cache_client = CacheClient(create_allocator(cache_scheme))
        self.backingstore_scheme = backingstore_scheme

        self.cache_client.reset()

    def request(self, tntid, key, write=False) -> Tuple[bool, float]:
        """ default write=False meaning GET request,
        return true if direct hit, false if fetching from backing store """
        val = self._genval()
        hit = self.cache_client.handle(tntid, key, val)
        additional_latency = 0 if hit else self._fetch(key)
        return hit, additional_latency

    def _fetch(self, _) -> float:
        """ return simulated latency of fetching data from backing store """
        return max(0.2, random.gauss(self.backingstore_scheme.latency_mu,
                                     self.backingstore_scheme.latency_sigma))

    def _genval(self, byte_size=1024) -> str:
        chars = [chr(random.randint(ord("a"), ord("z")))
                 for _ in range(byte_size)]
        return "".join(chars)
