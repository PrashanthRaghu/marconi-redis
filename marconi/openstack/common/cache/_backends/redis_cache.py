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

from marconi.openstack.common.cache import backends
from marconi.openstack.common import lockutils
import redis

CACHE_PREFIX = "cache_"
CACHE_PREFIX_PATTERN = "cache_*"


class RedisBackend(backends.BaseCache):
    """Stores the keys of the backend as a Redis hash.
       Id: <cache_key>

       value:

       Field name                value
       -------------------------------
       t                         ttl
       v                         value

       Each key is set to expire at the time of ttl
       using the built in expire command provided
       by redis.
    """

    def __init__(self, parsed_url, options=None):
        super(RedisBackend, self).__init__(parsed_url, options)
        # Use defaults for now. Let's override later.
        self._client = redis.StrictRedis()
        self._pipeline = self._client.pipeline()
        self._clear()

    def _reset_pipeline(self):
        self._pipeline.reset()

    def _set_unlocked(self, key, value, ttl, not_exists):
        pipeline = self._pipeline
        redis_key = gen_key(key)

        # Set the key to the provided value and call
        # expire at a time equal to ttl to auto expire
        # the cache value.
        cache_info = {
            'v': value,
            't': ttl
        }

        pipeline.hmset(redis_key, cache_info)
        pipeline.expire(redis_key, ttl)
        pipeline.execute()
        self._reset_pipeline()

    def _set(self, key, value, ttl, not_exists=False):
        with lockutils.lock(key):

            if not_exists and self._exists_unlocked(key):
                return False

            self._set_unlocked(key, value, ttl, not_exists)
            return True

    def _exists_unlocked(self, key):
        redis_key = gen_key(key)
        return self._client.get(redis_key) is not None

    def _get_unlocked(self, key, default=None):
        client = self._client
        redis_key = gen_key(key)
        cache_value = client.hgetall(redis_key)

        if not cache_value:
            return (0, default)

        return (int(cache_value['t']), cache_value['v'])

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
        with lockutils.lock(key):
            redis_key = gen_key(key)
            timeout, value = self._get_unlocked(redis_key)

            if value is None:
                return None

            cache_info = {
                'v': transform(value) + other
            }
            self._client.hmset(redis_key, cache_info)
            return cache_info['v']

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