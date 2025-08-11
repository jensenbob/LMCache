import os
import re
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


def parse_ip_port(ip_address:str) -> Tuple[str, int]:
    """
    parse addresses and ports

    :param str ip_address: The address to parse

    :return: A tuple containing the parsed address and the parsed port.

    :raises ValueError: If the port is not a valid port number.
    """
    if os.getenv('LM_USE_IPV6', '') != "1":
        ipv4_address, port = ip_address.split(":")
        logger.debug(f"parse ipaddress under ipv4 protocol, {ipv4_address}:{port}")
        return ipv4_address, int(port)

    pattern = r'^\[(.*)\]:(\d+)$'
    match = re.match(pattern, ip_address)

    if not match:
        raise ValueError(f"illegal ipv6 format: {ip_address}")

    ipv6_address = match.group(1)
    port = match.group(2)

    if not port.isdigit() or not (0 <= int(port) <= 65535):
        raise ValueError(f"illegal port: {port}")

    logger.debug(f"parse ipaddress under ipv6 protocol, [{ipv6_address}]:{port}")
    return ipv6_address, int(port)