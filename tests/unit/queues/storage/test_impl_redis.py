# Copyright (c) 2014 Prashanth Raghu.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import collections
import redis
import time
import uuid

from marconi import tests as testing
from marconi.openstack.common.cache import cache as oslo_cache
from marconi.openstack.common import timeutils
from marconi.queues.storage.redis import controllers
from marconi.queues.storage.redis import driver
from marconi.queues.storage.redis import options
from marconi.queues.storage.redis import utils
from marconi.queues import storage



class RedisUtilsTest(testing.TestBase):

    config_file = 'wsgi_redis.conf'

    def setUp(self):
        super(RedisUtilsTest, self).setUp()

        self.conf.register_opts(options.REDIS_OPTIONS,
                                group=options.REDIS_GROUP)

        self.redis_conf = self.conf[options.REDIS_GROUP]

        MockDriver = collections.namedtuple('MockDriver', 'redis_conf')

        self.driver = MockDriver(self.redis_conf)

    def test_scope_queue_name(self):
        self.assertEqual(utils.scope_queue_name('my-q'), '.my-q')
        self.assertEqual(utils.scope_queue_name('my-q', None), '.my-q')
        self.assertEqual(utils.scope_queue_name('my-q', '123'), '123.my-q')

        self.assertEqual(utils.scope_queue_name(None), '.')
        self.assertEqual(utils.scope_queue_name(None, '123'), '123.')

    def test_scope_messages_set(self):
        self.assertEqual(utils.scope_messages_set('my-q'), '.my-q.')
        self.assertEqual(utils.scope_messages_set('my-q', 'p'), 'p.my-q.')
        self.assertEqual(utils.scope_messages_set('my-q', 'p', 's'), 'p.my-q.s')

        self.assertEqual(utils.scope_messages_set(None), '..')
        self.assertEqual(utils.scope_messages_set(None, '123'), '123..')
        self.assertEqual(utils.scope_messages_set(None, None, 's'), '..s')

    def test_msg_claimed_filter(self):
        now = timeutils.utcnow_ts()

        msg = {
            'c': 'None',
            'c.e': now
        }

        self.assertFalse(utils.msg_claimed_filter(msg, now))

        msg = {
            'c': utils.generate_uuid(),
            'c.e': now + 60
        }

        self.assertTrue(utils.msg_claimed_filter(msg, now))

        msg['c.e'] = now - 60

        self.assertFalse(utils.msg_claimed_filter(msg, now))

    def test_msg_echo_filter(self):
        user_uid = uuid.uuid4().hex

        msg = {
            'u': user_uid
        }

        self.assertTrue(utils.msg_echo_filter(msg, user_uid))

        msg = {
            'u': uuid.uuid4().hex
        }

        self.assertFalse(utils.msg_echo_filter(msg, user_uid))

    def test_get_hostport(self):

        conn_id = "redis://datastructures.com:8369"

        self.assertEqual(utils.get_hostport(conn_id), ("datastructures.com", 8369))

    def test_basic_message(self):
        msg_id = uuid.uuid4().hex
        now = timeutils.utcnow_ts()

        msg = {
            'id': msg_id,
            'c.e': now - 5,
            't': '300',
            'b': 'Hello Earthlings',
        }

        basic_msg = utils.basic_message(msg, now)

        self.assertEqual(basic_msg['id'], msg_id)
        self.assertEqual(basic_msg['age'], 5)
        self.assertEqual(basic_msg['body'], 'Hello Earthlings')
        self.assertEqual(basic_msg['ttl'], '300')

    def test_retries_on_autoreconnect(self):
        num_calls = [0]

        @utils.retries_on_autoreconnect
        def _raises_autoreconnect(self):
            num_calls[0] += 1
            raise redis.exceptions.ConnectionError

        self.assertRaises(redis.exceptions.ConnectionError,
                          _raises_autoreconnect, self)
        self.assertEqual(num_calls, [self.redis_conf.max_reconnect_attempts])


@testing.requires_redis
class RedisDriverTest(testing.TestBase):

    config_file = 'wsgi_redis.conf'

    def test_db_instance(self):
        cache = oslo_cache.get_cache()
        redis_driver = driver.DataDriver(self.conf, cache)

        self.assertTrue(isinstance(redis_driver.connection, redis.StrictRedis))


@testing.requires_redis
class RedisQueuesTest(testing.TestBase):

    driver_class = driver.DataDriver
    config_file = 'wsgi_redis.conf'
    controller_class = controllers.QueueController

    def setUp(self):
        super(RedisQueuesTest, self).setUp()
        cache = oslo_cache.get_cache()
        self.driver = driver.DataDriver(self.conf, cache)
        self.connection = self.driver.connection
        self.q_controller = self.driver.queue_controller

    def tearDown(self):
        super(RedisQueuesTest, self).tearDown()
        self.connection.flushdb()

    def _prepare_conf(self):
        self.config(options.REDIS_GROUP,
                    database=10)

    def test_inc_counter(self):
        queue_name = 'inc-counter'

        q_controller = self.q_controller

        q_controller.create(queue_name)
        q_controller._inc_counter(queue_name, None, 10)

        scoped_q_name = utils.scope_queue_name(queue_name)
        self.assertEqual(self.connection.hgetall(scoped_q_name)['c'], '11')

    def test_inc_claimed(self):
        queue_name = 'inc-claimed'

        q_controller = self.q_controller

        q_controller.create(queue_name)
        q_controller._inc_claimed(queue_name, None, 10)

        scoped_q_name = utils.scope_queue_name(queue_name)
        self.assertEqual(self.connection.hgetall(scoped_q_name)['cl'], '10')

@testing.requires_redis
class RedisMessagesTest(testing.TestBase):
    driver_class = driver.DataDriver
    config_file = 'wsgi_redis.conf'
    controller_class = controllers.MessageController

    def setUp(self):
        super(RedisMessagesTest, self).setUp()
        cache = oslo_cache.get_cache()
        self.driver = driver.DataDriver(self.conf, cache)
        self.connection = self.driver.connection
        self.msg_controller = self.driver.message_controller
        self.q_controller = self.driver.queue_controller

    def tearDown(self):
        super(RedisMessagesTest, self).tearDown()
        self.connection.flushdb()

    def test_get_count(self):
        queue_name = 'get-count'
        self.q_controller.create(queue_name)

        msgs = [{
            'ttl': 300,
            'body': 'di mo fy'
        } for i in range(0, 10)]

        client_id = uuid.uuid4()
        # Creating 10 messages
        self.msg_controller.post(queue_name, msgs, client_id)

        messages_set_id = utils.scope_messages_set(queue_name, None,
                                                   'messages')

        num_msg = self.msg_controller._get_count(messages_set_id)
        self.assertEqual(num_msg, 10)

    def test_empty_queue_exception(self):
        queue_name = 'empty-queue-test'
        self.q_controller.create(queue_name)

        self.assertRaises(storage.errors.QueueIsEmpty,
                          self.msg_controller.first, queue_name)


@testing.requires_redis
class RedisClaimsTest(testing.TestBase):
    driver_class = driver.DataDriver
    config_file = 'wsgi_redis.conf'
    controller_class = controllers.ClaimController

    def setUp(self):
        super(RedisClaimsTest, self).setUp()
        cache = oslo_cache.get_cache()
        self.driver = driver.DataDriver(self.conf, cache)
        self.connection = self.driver.connection
        self.q_controller = self.driver.queue_controller
        self.claim_controller = self.driver.claim_controller

    def tearDown(self):
        super(RedisClaimsTest, self).tearDown()
        self.connection.flushdb()

    def test_claim_doesnt_exist(self):
        queue_name = 'no-such-claim'
        epoch = '000000000000000000000000'
        self.q_controller.create(queue_name)
        self.assertRaises(storage.errors.ClaimDoesNotExist,
                          self.claim_controller.get, queue_name,
                          epoch, project=None)

        claim_id, messages = self.claim_controller.create(queue_name,
                                                          {'ttl': 2, 'grace': 0},
                                                          project=None)

        # Lets let it expire
        time.sleep(2)
        self.assertRaises(storage.errors.ClaimDoesNotExist,
                          self.claim_controller.update, queue_name,
                          claim_id, {}, project=None)


class RedisShardsTest(testing.TestBase):
    driver_class = driver.DataDriver
    config_file = 'wsgi_redis.conf'
    controller_class = controllers.ShardsController

    def setUp(self):
        super(RedisShardsTest, self).setUp()

    def test_normalize(self):
        from marconi.queues.storage.redis import shards

        s_info = {
            'n': 's',
            'u': 'proto://uri:port',
            'w': '100',
            'o': {'plenty': 'of options'}
        }

        ns_info = shards._normalize(s_info)

        self.assertEqual(ns_info['name'], 's')
        self.assertEqual(ns_info['uri'], 'proto://uri:port')
        self.assertEqual(ns_info['weight'], 100)
        self.assertEqual(ns_info.get('options'), None)

        ns_info = shards._normalize(s_info, detailed=True)

        self.assertEqual(ns_info.get('options'), {'plenty': 'of options'})
