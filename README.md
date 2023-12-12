To run a test, first ensure there is a redis server. Update redis host and port in service/client.py.
Then you can run main.py after installing all requirement dependencies.
Cache traces for testing are in data/trace. Tenants configuration is coupled in it.
Feel free to play with parameters `latency_mu`, `cache_ratio`, and `allocator_class` specified in main.py.  

