from typing import Sequence, Tuple

from lmcache.utils import CacheEngineKey
from lmcache.v1.memory_management import MemoryObj
from lmcache.logging import init_logger


logger = init_logger(__name__)


def distribute_tuple_list(
    key_memory_tuple: Sequence[Tuple[CacheEngineKey, MemoryObj]],
    active_peers: Sequence[str]
    ) -> Sequence[Tuple[Sequence[Tuple[CacheEngineKey, MemoryObj]], str]]:
    """
    Distribute the list of tuples consisting of key and memobj evenly among the available peers.

    :param Sequence[Tuple[CacheEngineKey, MemoryObj]] key_memory_tuple: The list of tuples to be distributed.

    :param Sequence[str] active_peers: The list of active peers.

    :return: A tuple containing the distributed list of tuples and the chosen peer.

    """
    n = len(key_memory_tuple)
    m = len(active_peers)
    if n == 0:
        return []

    if m == 0:
        logger.warning("no active peers available")
        return []


    base = n // m
    remainder = n % m
    result = []
    start = 0
    for i in range(m):
        count = base + 1 if i < remainder else base
        end = start + count
        sub_list = key_memory_tuple[start:end]
        result.append((sub_list, active_peers[i]))
        start = end

    return result
