# Copyright 2014 Prashanth Raghu
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import redis

from six.moves.urllib import parse
from marconi.openstack.common.cache import backends
from marconi.openstack.common import lockutils

CACHE_PREFIX = "cache_"
CACHE_PREFIX_PATTERN = "cache_*"


class RedisBackend(backends.BaseCache):
    """Stores the keys of the backend as a Redis hash.
       Id: <cache_key> ------------- <value>

       Each key is set to expire at the time of ttl
       using the built in expire command provided
       by redis.
    """

    def __init__(self, parsed_url, options=None):
        super(RedisBackend, self).__init__(parsed_url, options)
        # Use defaults for now. Let's override later.
        host, port = parsed_url.netloc.split(":")
        self._client = redis.StrictRedis(host=host, port=int(port))
        self._pipeline = self._client.pipeline()
        self._clear()

    def _reset_pipeline(self):
        self._pipeline.reset()

    def _set_unlocked(self, key, value, ttl):
        pipeline = self._pipeline
        redis_key = gen_key(key)

        pipeline.delete(redis_key)
        if isinstance(value, list):
            for element in value:
                pipeline.rpush(redis_key, element)
        else:
            pipeline.set(redis_key, value)

        pipeline.expire(redis_key, ttl)
        pipeline.execute()
        self._reset_pipeline()

    def _set(self, key, value, ttl, not_exists=False):
        with lockutils.lock(key):

            if not_exists and self._exists_unlocked(key):
                return False

            self._set_unlocked(key, value, ttl)
            return True

    def _exists_unlocked(self, key):
        redis_key = gen_key(key)
        return self._client.exists(redis_key)

    def _get_unlocked(self, key, default=None):
        client = self._client
        redis_key = gen_key(key)

        try:
            cache_value = client.get(redis_key)
            cache_value = int_transform(cache_value)
        except redis.exceptions.ResponseError:
            cache_value = [int_transform(x) for x in client.lrange(redis_key, 0, -1)]

        if not cache_value:
            return (0, default)

        return (client.ttl(redis_key), cache_value)

    def _get(self, key, default):
        with lockutils.lock(key):
            return self._get_unlocked(key, default)[1]

    def __delitem__(self, key):
        with lockutils.lock(key):
            redis_key = gen_key(key)
            self._client.delete(redis_key)

    def _clear(self):
        """Implementation uses scan with the prefix
           to delete keys until the cursor returns 0
           to end the iteration.
        """
        pipe = self._pipeline
        client = self._client
        cursor = 0

        while True:
            cursor, keys = client.scan(match=CACHE_PREFIX_PATTERN,
                                       cursor=cursor)
            if cursor == '0':
                break

            for key in keys:
                pipe.delete(key)

        pipe.execute()
        self._reset_pipeline()

    def _incr(self, key, delta):
        if not isinstance(delta, int):
            raise TypeError('delta must be an int instance')

        return self._incr_append(key, delta, transform=int)

    def _incr_append(self, key, other, transform=int):
        client = self._client
        with lockutils.lock(key):
            redis_key = gen_key(key)
            timeout, value = self._get_unlocked(key)

            if value is None:
                return None

            if isinstance(value, list):
                for element in other[0]:
                    client.rpush(redis_key, element)

                return client.lrange(redis_key, 0, -1)
            else:
                client.incrby(redis_key, other)
                return client.get(redis_key)

    def _append_tail(self, key, tail):
        return self._incr_append(key, tail)

    def __contains__(self, key):
        return self._exists_unlocked(key)

    def _get_many(self, keys, default):
        return super(RedisBackend, self)._get_many(keys, default)

    def _set_many(self, data, ttl=0):
        return super(RedisBackend, self)._set_many(data, ttl)

    def _unset_many(self, keys):
        return super(RedisBackend, self)._unset_many(keys)


def gen_key(key):
    """Generates the redis key for storing
       the value in the cache.
    """
    return CACHE_PREFIX + key

def int_transform(x):
    try:
        return int(x)
    except ValueError:
        return x

if __name__ == "__main__":
    parsed = parse.urlparse("redis://127.0.0.1:6379")
    cache = RedisBackend(parsed, {})
    cache.set_many({'a': 46, 'b': [1, 2, 3], 'k': [1, 2, '3', 'four']}, 300)
    cache.get_many(['a', 'b', 'k']) 