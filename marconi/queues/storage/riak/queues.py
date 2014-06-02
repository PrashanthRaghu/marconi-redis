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
from marconi.queues.storage.riak import utils
from marconi.queues.storage import errors
from marconi.openstack.common import timeutils

LOG = logging.getLogger(__name__)
QUEUE_MODTIME_INDEX = 'q_modtime_int'

class QueueController(storage.Queue):
    """Implements queue resource operations using Riak.

    Queues are scoped by project, which is prefixed to the
    queue name.

    Queue Information (Redis Hash):

        Id: <project-id_q-name>
        Id                     Value
        --------------------------------
        c               ->     count
        m               ->     metadata
        t               -> created / updated time

    """
    @utils.cache_bucket
    def _get_project_bkt(self, project):
        bkt_name = utils.scope_queue_bucket(project)
        bkt = self._driver.bucket(bkt_name)
        bkt.set_properties({'allow_mult': False,
                            'last_write_wins': True})
        return bkt


    def __init__(self, *args, **kwargs):
        super(QueueController, self).__init__(*args, **kwargs)
        self._driver = self.driver.connection
     
    def list(self, project=None, marker=None,
             limit=storage.DEFAULT_QUEUES_PER_PAGE, detailed=False):

        start = marker or None
        limit = limit if limit else storage.DEFAULT_MESSAGES_PER_CLAIM
        project_bkt = self._get_project_bkt(project)
        now = timeutils.utcnow_ts()

        results_obj = project_bkt.get_index(QUEUE_MODTIME_INDEX, 0,
                                        continuation=start, max_results=limit,
                                        endkey=now)
        result_keys = results_obj.results

        def _it():
            for q_name in result_keys:
                queue = project_bkt.get(q_name).data
                yield _normalize(queue, q_name, detailed)

        yield _it()
        yield results_obj.continuation

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    def get_metadata(self, name, project=None):
        if not self.exists(name, project):
            raise errors.QueueDoesNotExist(name, project)

        q_name = utils.scope_queue_name(name, project)
        project_bkt = self._get_project_bkt(project)

        q_info = project_bkt.get(q_name).data
        return q_info['m']


    @utils.raises_conn_error
    def create(self, name, project=None):
        q_name = utils.scope_queue_name(name, project)
        project_bkt = self._get_project_bkt(project)

        if self.exists(name, project):
            return False

        q_info = {
            'c': 1,
            'm': {},
            't': timeutils.utcnow_ts()
        }
        # Riak python client supports automatic
        # serialization of py dicts <-> JSON.
        q_obj = project_bkt.new(q_name, q_info)
        q_obj.add_index(QUEUE_MODTIME_INDEX, q_info['t'])
        q_obj.store()
        return True

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    def exists(self, name, project=None):
        # The exists attribute of the Riak Object
        # confirms the existance of the queue.
        q_name = utils.scope_queue_name(name, project)
        project_bkt = self._get_project_bkt(project)
        return project_bkt.get(q_name).exists

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    def set_metadata(self, name, metadata, project=None):
        if not self.exists(name, project):
            raise errors.QueueDoesNotExist(name, project)

        q_name = utils.scope_queue_name(name, project)
        project_bkt = self._get_project_bkt(project)

        q_info = project_bkt.get(q_name)
        q_data = q_info.data
        # Set the new metadata.
        q_data['m'] = metadata
        q_data['t'] = timeutils.utcnow_ts()
        # Store the back into the bucket.
        # Note(prashanthr_): Check if re-indexing is needed.
        q_info.add_index(QUEUE_MODTIME_INDEX, q_data['t'])
        q_info.store()

    @utils.raises_conn_error
    @utils.retries_on_autoreconnect
    def delete(self, name, project=None):
        if not self.exists(name, project):
            raise errors.QueueDoesNotExist(name, project)

        q_name = utils.scope_queue_name(name, project)
        project_bkt = self._get_project_bkt(project)
        # Delete ensures the removal of the queue from the
        # project bucket.
        project_bkt.delete(q_name)

    @utils.raises_conn_error
    def stats(self, name, project=None):
        raise NotImplementedError

def _normalize(queue_obj, q_name, detailed):
    q_info = {
        'name': utils.descope_queue_name(q_name),
        'metadata': queue_obj['m']
    }

    if not detailed:
        del q_info['metadata']

    return q_info