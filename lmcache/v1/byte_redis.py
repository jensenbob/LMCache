import bytedredis

redis_psm = "toutiao.redis.caijing_ai_robot"

class ByteRedis:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
        return cls._instance

    # 实例化redis
    @classmethod
    def get_conn(cls):
        url = f"redis://?db=0&redis_psm={redis_psm}&socket_connect_timeout=0.25&socket_timeout=0.3"
        client = bytedredis.Client.from_url(url)
        return client

    @classmethod
    def get_psm(cls):
        return redis_psm
