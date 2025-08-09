# SPDX-License-Identifier: Apache-2.0
# Standard
import os
import time
from typing import Optional, Sequence, Tuple
import inspect

# Third Party
import redis

# First Party
from lmcache.logging import init_logger
from lmcache.utils import CacheEngineKey
from lmcache.v1.config import LMCacheEngineConfig
from lmcache.v1.lookup_server.abstract_server import LookupServerInterface  # noqa: E501
from lmcache.v1.tools import parse_ip_port

logger = init_logger(__name__)

ACTIVE_PEERS = "ACTIVE_PEERS"

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

    def lookup(self, key: CacheEngineKey) -> Optional[Tuple[str, int]]:
        """
        Perform lookup in the lookup server.
        """
        logger.debug("Call to lookup in lookup server")
        url = self.connection.get(key.to_string())
        logger.debug(f"KV cache lives on {url}")
        assert not inspect.isawaitable(url)
        if url is None:
            return None
        host, port = url.split(":")
        return host, int(port)

    def insert(self, key: CacheEngineKey):
        """
        Perform insert in the lookup server.
        """
        assert self.distributed_url is not None
        logger.debug("Call to insert in lookup server")
        self.connection.set(key.to_string(), self.distributed_url)
        self.connection.zadd(ACTIVE_PEERS, {self.distributed_url: int(time.time())})

    def batched_insert(self, keys: Sequence[CacheEngineKey]):
        """
        Perform batched insert in the lookup server.
        """
        assert self.distributed_url is not None
        logger.debug("Call to batched insert in lookup server")

        # TODO(Jiayi): Optimize this with redis pipe
        for key in keys:
            self.connection.set(key.to_string(), self.distributed_url)
        self.connection.zadd(ACTIVE_PEERS, {self.distributed_url: int(time.time())})

    def remove(self, key: CacheEngineKey):
        """
        Perform remove in the lookup server.
        """
        logger.debug("Call to remove in lookup server")
        self.connection.delete(key.to_string())
        self.connection.zadd(ACTIVE_PEERS, {self.distributed_url: int(time.time())})

    def batched_remove(self, keys: Sequence[CacheEngineKey]):
        """
        Perform batched remove in the lookup server.
        """
        logger.debug("Call to batched remove in lookup server")
        # TODO(Jiayi): We might need to cache the `str_keys` for performance.
        str_keys = [key.to_string() for key in keys]
        self.connection.delete(*str_keys)
        self.connection.zadd(ACTIVE_PEERS, {self.distributed_url: int(time.time())})

    def active_peers(self) -> Sequence[str]:
        """
        Perform active_peers in the lookup server.
        """
        logger.debug("Call to active_peers in lookup server")
        peers = self.connection.zrange("rank", 0, -1, withscores=True)
        valid_peers = []
        invalid_peers = []
        for peer, heartbeat in peers:
            if time.time() - heartbeat > 60:  # delete expired peers
                invalid_peers.append(peer)
            else:
                valid_peers.append(peer)

        # TODO: Optimize this with redis pipe and asyncio
        if len(invalid_peers) > 0:
            self.connection.zrem(ACTIVE_PEERS, invalid_peers)
        logger.debug(f"Valid peers: {valid_peers}")
        return valid_peers
