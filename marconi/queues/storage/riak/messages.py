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

import six
import datetime
from marconi.queues import storage
from marconi.queues.storage.redis import utils
from marconi.openstack.common import timeutils
from marconi.queues.storage import errors
from collections import defaultdict

QUEUE_MESSAGES_LIST_SUFFIX = 'messages'

class MessageController(storage.Message):
    """Implements message resource operations using Riak.

    Messages are scoped by project + queue.

    Redis Data Structures:
    ---------------------
    Messages list ( Redis Sorted Set ) contains message ids
    sorted by timestamp.

    scope: <project-id_q-name>

        Name                Field
        -------------------------
        message_ids           m

    Messages(Redis Hash):
        Name                Field
        -------------------------
        ttl              ->     t
        expires          ->     e
        body             ->     b
        claim            ->     c
        client uuid      ->     u
    """
    def _init_pipeline(self):
        self._pipeline = self._client.pipeline()

    def init_connection(self):
        """
            Will be used during reconnection attempts.
        """
        self._client = self.driver.connection

    def _active(self, queue_name, marker=None, echo=False,
                client_uuid=None, project=None,
                limit=None):
        return self.list(queue_name, project=project, marker=marker,
                         echo=echo, client_uuid=client_uuid,
                         include_claimed=False,
                         limit=limit)

    def _get_count(self, messages_set_id):
        # Return the number of messages in a queue scoped by
        # queue and project.
        return self._client.zcard(messages_set_id)

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    @utils.reset_pipeline
    def _delete_queue_messages(self, queue, project):
        # Method to remove all the messages belonging to an
        # individual queue. Will be referenced from the
        # QueueController.
        client = self._client
        pipe = self._pipeline
        messages_set_id = utils.scope_messages_set(queue, project,
                                        QUEUE_MESSAGES_LIST_SUFFIX)
        message_ids = client.zrange(messages_set_id, 0, -1)

        pipe.delete(messages_set_id)
        for msg_id in message_ids:
            pipe.delete(msg_id)

        pipe.execute()


    def _exists(self, queue, project, key):
        # Helper function which checks if a particular message_id
        # exists in the sorted set of the queues message ids.
        # Note(prashanthr_): Operation of the order of O(n)

        messages_set_id = utils.scope_messages_set(queue, project,
                                        QUEUE_MESSAGES_LIST_SUFFIX)

        return self._client.zrank(messages_set_id, key) is not None

    def _get_first_message_id(self, queue, project, sort):
        # Helper function to get the first message in the queue
        # sort > 0 get from the left else from the right.

        messages_set_id = utils.scope_messages_set(queue, project,
                                        QUEUE_MESSAGES_LIST_SUFFIX)

        if sort > 0:
            return self._client.zrange(messages_set_id, 0, 0)

        return self._client.zrevrange(messages_set_id, 0, 0)

    def _get(self, message_id):
        return self._client.hgetall(message_id)

    def __init__(self, *args, **kwargs):
        super(MessageController, self).__init__(*args, **kwargs)
        self.init_connection()
        self._init_pipeline()
        self._queue_controller = self.driver.queue_controller

    def list(self, queue, project=None, marker=None,
             limit=storage.DEFAULT_MESSAGES_PER_PAGE,
             echo=True, client_uuid=None,
             include_claimed=False):

        if not self._queue_controller.exists(queue, project):
            raise errors.QueueDoesNotExist(queue,
                                             project)

        messages_set_id = utils.scope_messages_set(queue, project,
                                        QUEUE_MESSAGES_LIST_SUFFIX)
        client = self._client
        start = client.zrank(messages_set_id, marker) or 0
        # In a sharded context, the default values are not being set.
        limit = limit or storage.DEFAULT_MESSAGES_PER_CLAIM
        message_ids = client.zrange(messages_set_id, start, start+limit)
        filters = defaultdict(dict)

        # Build a list of filters for checking the following:
        # 1. Message is claimed.
        # 2. echo message to the client.
        if not include_claimed:
            filters['claimed_filter']['f'] = utils.msg_claimed_filter
            filters['claimed_filter']['f.v'] = timeutils.utcnow_ts()

        if not echo:
            filters['echo_filter']['f'] = utils.msg_echo_filter
            filters['echo_filter']['f.v'] = client_uuid

        marker = {}
        def _it(message_ids, filters={}):
        # The function accepts a list of filters
        # to be filtered before the the message
        # can be included as a part of the reply.
            now = timeutils.utcnow_ts()

            for message_id in message_ids:
                message = client.hgetall(message_id)
                if message:
                    for filter in six.itervalues(filters):
                        filter_func = filter['f']
                        filter_val = filter['f.v']
                        if filter_func(message, filter_val):
                            break
                    else:
                        marker['next'] = message['k']
                        yield utils.basic_message(message, now)

        yield _it(message_ids, filters)
        yield marker['next']

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    def first(self, queue, project=None, sort=1):
        message_id = self._get_first_message_id(queue, project, sort)
        return self.get(queue, message_id, project)

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    def get(self, queue, message_id, project=None):
        if not self._queue_controller.exists(queue, project):
            raise errors.QueueDoesNotExist(queue,
                                             project)

        if not self._exists(queue, project, message_id):
            raise errors.MessageDoesNotExist(message_id, queue,
                                             project)

        msg = self._get(message_id)
        return utils.basic_message(msg, timeutils.utcnow_ts())

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    def bulk_get(self, queue, message_ids, project=None):
        # TODO: Pipeline is not used here as atomic guarentee
        # is not required.but can be used for performance.
        client = self._client
        if not self._queue_controller.exists(queue, project):
                raise errors.QueueDoesNotExist(queue,
                                             project)

        def _it(message_ids):
            now = timeutils.utcnow_ts()

            for message_id in message_ids:
                message = client.hgetall(message_id)
                if message:
                    yield utils.basic_message(message, now)

        return _it(message_ids)

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    @utils.reset_pipeline
    def post(self, queue, messages, client_uuid, project=None):
        if not self._queue_controller.exists(queue, project):
            raise errors.QueueDoesNotExist(queue,
                                             project)

        messages_set_id = utils.scope_messages_set(queue, project,
                                        QUEUE_MESSAGES_LIST_SUFFIX)

        now = timeutils.utcnow_ts()
        now_dt = datetime.datetime.utcfromtimestamp(now)
        message_ids = []
        num_messages = self._get_count(messages_set_id)
        prepared_messages = [
            {
                'id': utils.generate_uuid(),
                't': message['ttl'],
                'e': now_dt + datetime.timedelta(seconds=message['ttl']),
                'u': client_uuid,
                'c': None,
                'k': num_messages + index,
                'c.e': now,
                'b': message['body'] if 'body' in message else {},
            }
            for index, message in enumerate(messages)
        ]

        pipe = self._pipeline
        self._queue_controller._inc_counter(queue, project, len(messages))

        for message in prepared_messages:
            m_id = message['id']
            message_ids.append(m_id)
            pipe.zadd(messages_set_id, message['c.e'], m_id)
            pipe.hmset(m_id, message)

        pipe.execute()
        return message_ids

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    @utils.reset_pipeline
    def delete(self, queue, message_id, project=None, claim=None):
        messages_set_id = utils.scope_messages_set(queue, project,
                                        QUEUE_MESSAGES_LIST_SUFFIX)

        if not self._queue_controller.exists(queue, project):
            raise errors.QueueDoesNotExist(queue,
                                             project)

        if not self._exists(queue, project, message_id):
            raise errors.MessageDoesNotExist(message_id, queue,
                                             project)

        now = timeutils.utcnow_ts()
        message = self._get(message_id)
        msg_claim = message['c']

        is_claimed = (msg_claim is not 'None' and
                      int(message['c.e']) > now)

        if claim is None:
            if is_claimed:
                raise errors.MessageIsClaimed(message_id)
        else:
            if msg_claim != claim:
                raise errors.MessageIsClaimedBy(message_id, msg_claim)

        self._pipeline.delete(message_id).\
            zrem(messages_set_id, message_id).\
            execute()

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    @utils.reset_pipeline
    def bulk_delete(self, queue, message_ids, project=None):
        if not self._queue_controller.exists(queue, project):
            raise errors.QueueDoesNotExist(queue,
                                             project)

        messages_set_id = utils.scope_messages_set(queue, project,
                                        QUEUE_MESSAGES_LIST_SUFFIX)
        pipe = self._pipeline

        for message_id in message_ids:
            pipe.delete(message_id).\
            zrem(messages_set_id, message_id)

        pipe.execute()



