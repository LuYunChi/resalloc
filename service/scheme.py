class CacheScheme:
    def __init__(self, cache_size, allocator_class) -> None:
        self.cache_size = cache_size
        self.allocator_class = allocator_class


class BackingStoreScheme:
    def __init__(self, latency_mu, latency_sigma) -> None:
        self.latency_mu = latency_mu
        self.latency_sigma = latency_sigma
