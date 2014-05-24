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

from marconi.openstack.common import log as logging
from marconi.queues import storage
from marconi.queues.storage.redis import utils
from marconi.queues.storage import errors
from marconi.openstack.common import timeutils

LOG = logging.getLogger(__name__)

QUEUES_SET_STORE_NAME = 'queues_set'

class QueueController(storage.Queue):
    """Implements queue resource operations using Redis.

    Queues are scoped by project, which is prefixed to the
    queue name.

    Queues ( Redis Sorted Set ):
        Id: queues_set

        Id                   Value
        ---------------------------------
        name      ->   <project-id_q-name>


    The set helps faster existence checks, while the list helps
    paginated retrieval of queues.

    Queue Information (Redis Hash):

        Id: <project-id_q-name>
        Id                     Value
        --------------------------------
        c               ->     count
        m               ->     metadata

    """

    def _init_pipeline(self):
        self._pipeline = self._client.pipeline()

    def init_connection(self):
        """
            Will be used during reconnection attempts.
        """
        self._client = self.driver.connection

    def _inc_counter(self, name, project, amount=1):
        q_id = utils.scope_queue_name(name, project)
        count = self._get_queue_info(q_id , 'c', int)
        self._set_counter(q_id, count+amount)

    def _set_counter(self, q_id, count):
        q_info = {
            'c': count,
            'm': self._get_queue_info(q_id, 'm'),
            't': timeutils.utcnow_ts()
        }
        self._client.hmset(q_id, q_info)


    def __init__(self, *args, **kwargs):
        super(QueueController, self).__init__(*args, **kwargs)
        self.init_connection()
        self._init_pipeline()

    def _get_queue_info(self, q_id, field, transform=str):
        """
            Method to retrieve a particular field from
            the queue information.
        """
        return transform(self._client.hgetall(q_id)[field])

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    def list(self, project=None, marker=None,
             limit=storage.DEFAULT_QUEUES_PER_PAGE, detailed=False):
        client = self._client
        qset_id = utils.scope_queue_name(QUEUES_SET_STORE_NAME, project)
        marker = utils.scope_queue_name(marker, project)
        start = client.zrank(qset_id, marker) or 0

        cursor = (q for q in client.zrange(qset_id, start, start+limit))
        marker_next = {}

        def denormalizer(q_info, q_name):
            queue = {'name': utils.descope_queue_name(q_name)}
            marker_next['next'] = queue['name']
            if detailed:
                queue['metadata'] = q_info[1]

            return queue

        yield utils.QueueListCursor(self._client, cursor, denormalizer)
        yield marker_next and marker_next['next']

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    def get_metadata(self, name, project=None):
        if not self.exists(name, project):
            raise errors.QueueDoesNotExist(name, project)

        q_id = utils.scope_queue_name(name, project)
        metadata = self._get_queue_info(q_id, 'm')
        return {} if utils.is_metadata_empty(metadata) else metadata

    @utils.raises_conn_error
    @utils.reset_pipeline
    def create(self, name, project=None):
        #Note (prashanthr_): Implement as a lua script.
        q_id = utils.scope_queue_name(name, project)
        qset_id = utils.scope_queue_name(QUEUES_SET_STORE_NAME, project)

        pipe = self._pipeline
        # Check if the queue already exists.
        if self._client.zrank(qset_id, q_id) is not None:
            return False

        # Pipeline ensures atomic inserts.
        q_info = {
            'c': 1,
            'm': {},
            't': timeutils.utcnow_ts()
        }

        pipe.zadd(qset_id, 1, q_id)\
            .hmset(q_id, q_info)

        return all(map(bool, pipe.execute()))

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    def exists(self, name, project=None):
        # Note (prashanthr_): try to fit in caching if possible.
        q_id = utils.scope_queue_name(name, project)
        qset_id = utils.scope_queue_name(QUEUES_SET_STORE_NAME, project)

        return not self._client.zrank(qset_id, q_id) is None

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    def set_metadata(self, name, metadata, project=None):
        if not self.exists(name, project):
            raise errors.QueueDoesNotExist(name, project)

        q_id = utils.scope_queue_name(name, project)
        q_info = {
            'c': 1,
            'm': metadata,
            't': timeutils.utcnow_ts()
        }

        if self.exists(name, project):
            q_info['c'] = self._get_queue_info(q_id, 'c')

        self._client.hmset(q_id, q_info)

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    @utils.reset_pipeline
    def delete(self, name, project=None):
        # Pipelining is used to ensure no race conditions
        # occur.
        if not self.exists(name, project):
            raise errors.QueueDoesNotExist(name, project)

        q_id = utils.scope_queue_name(name, project)
        qset_id = utils.scope_queue_name(QUEUES_SET_STORE_NAME, project)

        pipe = self._pipeline

        pipe.zrem(qset_id, q_id)
        pipe.delete(q_id)

        #Note(prashanthr_): Delete all messages in the queue later.
        pipe.execute()


    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    def stats(self, name, project=None):
        # TODO: Depends on claims controller.
        raise NotImplementedError
