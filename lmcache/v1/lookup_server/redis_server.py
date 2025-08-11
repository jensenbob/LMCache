# SPDX-License-Identifier: Apache-2.0
# Standard
import threading
import time
from typing import Optional, Sequence, Tuple
import inspect

# Third Party
import redis
import schedule

# First Party
from lmcache.logging import init_logger
from lmcache.utils import CacheEngineKey
from lmcache.v1.config import LMCacheEngineConfig
from lmcache.v1.lookup_server.abstract_server import LookupServerInterface  # noqa: E501
from lmcache.v1.tools import parse_ip_port

logger = init_logger(__name__)

ACTIVE_PEERS = "LOCAL_MODEL:LMCACHE:P2P:ACTIVE_PEERS"
MAX_HEARTBEAT_DELAY = 60  # seconds

def background_heartbeat():
    while True:
        schedule.run_pending()
        time.sleep(5)

# TODO (Jiayi): Batching is needed for Redis lookup server.
class RedisLookupServer(LookupServerInterface):
    def __init__(self, config: LMCacheEngineConfig):
        self.distributed_url = config.distributed_url
        assert self.distributed_url is not None

        self.url = config.lookup_url
        assert self.url is not None

        host, port = parse_ip_port(self.url)
        self.host = host
        self.port = int(port)

        self.connection = redis.Redis(
            host=self.host, port=self.port, decode_responses=True
        )
        logger.info(f"Connected to Redis lookup server at [{host}]:{port}")
        # decode_responses=False)

        schedule.every(1).seconds.do(self.heartbeat)  # 每3秒执行一次
        scheduler_thread = threading.Thread(
            target=background_heartbeat,
            daemon=True  # 守护线程：主程序退出时自动结束
        )
        scheduler_thread.start()
        logger.info("Started background heartbeat thread")

    def lookup(self, key: CacheEngineKey) -> Optional[Tuple[str, int]]:
        """
        Perform lookup in the lookup server.
        """
        logger.debug("Call to lookup in lookup server")
        lua_script = """
           local cache_engine_key = KEYS[1]
           local active_peers_key = KEYS[2]
           local now = ARGV[1]
           local max_delay = ARGV[2]
           local url = redis.call('GET', cache_engine_key)
           if url == nil then
               return nil, nil
           end
           local score = redis.call('ZSCORE', active_peers_key, url)
           if now - score > max_delay then
               redis.call('zrem', active_peers_key, url)
               return nil, nil
           end
           return url, score
           """
        url, score = self.connection.execute_script(lua_script, key.to_string(), ACTIVE_PEERS, int(time.time()), MAX_HEARTBEAT_DELAY)
        logger.debug(f"Redis lus executed. url:{url}, score:{score}")
        if url is None:
            return None
        if url == self.distributed_url:
            logger.debug(f"Shouldn't find on itself, target_url:{url}, key:{key.to_string()}")
            return None

        assert not inspect.isawaitable(url)

        logger.debug(f"Key{key} lives on peers{url}")
        host, port = parse_ip_port(url)
        return host, int(port)

    def insert(self, key: CacheEngineKey):
        """
        Perform insert in the lookup server.
        """
        assert self.distributed_url is not None
        logger.debug("Call to insert in lookup server")
        self.connection.set(key.to_string(), self.distributed_url)

    def batched_insert(self, keys: Sequence[CacheEngineKey]):
        """
        Perform batched insert in the lookup server.
        """
        if len(keys) == 0:
            return

        assert self.distributed_url is not None
        logger.debug("Call to batched insert in lookup server")

        # TODO(Jiayi): Optimize this with redis pipe
        pipe = self.connection.pipeline()
        for key in keys:
            pipe.set(key.to_string(), self.distributed_url)
        pipe.execute()

    def remove(self, key: CacheEngineKey):
        """
        Perform remove in the lookup server.
        """
        logger.debug("Call to remove in lookup server")
        self.connection.delete(key.to_string())

    def batched_remove(self, keys: Sequence[CacheEngineKey]):
        """
        Perform batched remove in the lookup server.
        """
        logger.debug("Call to batched remove in lookup server")
        # TODO(Jiayi): We might need to cache the `str_keys` for performance.
        str_keys = [key.to_string() for key in keys]
        self.connection.delete(*str_keys)

    def heartbeat(self):
        """
        Perform update heartbeat for current pod.
        """
        self.connection.zadd(ACTIVE_PEERS, {self.distributed_url: int(time.time())})
        logger.debug(f"Heartbeat from {self.distributed_url} for {ACTIVE_PEERS} in lookup server")

    def active_peers(self) -> Sequence[str]:
        """
        Perform active_peers in the lookup server.
        """
        logger.debug("Call to active_peers in lookup server")

        lua_script = """
            local key = KEYS[1]
            local now = ARGV[1]
            local max_delay = ARGV[2]
            
            local all_entries = redis.call('ZRANGE', key, 0, -1, 'WITHSCORES')
            local inactive_peers = {}
            local active_peers = {}

            for i = 1, #all_entries, 2 do
                local member = all_entries[i]
                local last_heartbeat = tonumber(all_entries[i+1])

                if now - last_heartbeat > max_delay then
                    table.insert(inactive_peers, member)
                else
                    table.insert(active_peers, member)
                end
            end

            if #inactive_peers > 0 then
                redis.call('ZREM', key, unpack(inactive_peers))
            end

            return active_peers
            """
        active_peers = self.connection.execute_script(lua_script, ACTIVE_PEERS, int(time.time()), MAX_HEARTBEAT_DELAY)
        if self.distributed_url not in active_peers:
            logger.error(f"Self url {self.distributed_url} not in active peers")
            return []

        logger.debug(f"Valid peers: {active_peers}")
        active_peers.remove(self.distributed_url)
        return active_peers