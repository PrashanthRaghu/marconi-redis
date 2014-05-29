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
"""Riak storage controller for the queues ctlg.

Serves to construct an association between a project + queue -> shard

    Tracked shard ctlgs:
    ------------------------
    Redis id: (ctlg)

    Redis Data Structure:
    ---------------------
    Id                                     Value
    --------------------------------------------------
    'project_queue_ctlg'   ->             'shard'
"""
from marconi.queues.storage import base
from marconi.queues.storage.redis import utils
from marconi.queues.storage import errors

QUEUE_CATALOGUE_SUFFIX = 'catalogue'

class CatalogueController(base.CatalogueBase):

    def __init__(self, *args, **kwargs):
        super(CatalogueController, self).__init__(*args, **kwargs)
        self._client = self.driver.connection
        self._pipeline = self._client.pipeline()

    @utils.raises_conn_error
    def list(self, project):
        client = self._client
        # Get the list of all ctlg entries.
        # NOTE(prashanthr_): Not being used from the ctlg
        ctlg_entries = client.zrange(QUEUE_CATALOGUE_SUFFIX, 0, -1)
        entries = []

        for ctlg_entry in ctlg_entries:
            project, queue = utils.descope_from_catalogue(ctlg_entry)
            shard = client.get(ctlg_entry)
            entries.append(_normalize(project, queue, shard))

        return entries

    @utils.raises_conn_error
    def get(self, project, queue):
        ctlg_name = utils.scope_queue_catalogue(queue
                                    , project, QUEUE_CATALOGUE_SUFFIX)
        shard = self._client.get(ctlg_name)
        if shard is None:
            raise errors.QueueNotMapped(project, queue)

        return {"shard":
                    self._client.get(ctlg_name)}

    @utils.raises_conn_error
    def exists(self, project, queue):
        ctlg_name = utils.scope_queue_catalogue(queue
                                    , project, QUEUE_CATALOGUE_SUFFIX)
        return self._client.zrank(QUEUE_CATALOGUE_SUFFIX, ctlg_name) is not None

    @utils.raises_conn_error
    @utils.reset_pipeline
    def insert(self, project, queue, shard):
        pipe = self._pipeline
        ctlg_name = utils.scope_queue_catalogue(queue
                                    , project, QUEUE_CATALOGUE_SUFFIX)
        pipe.zadd(QUEUE_CATALOGUE_SUFFIX, 1, ctlg_name)
        pipe.set(ctlg_name, shard)
        pipe.execute()

    @utils.raises_conn_error
    @utils.reset_pipeline
    def delete(self, project, queue):
        pipe = self._pipeline
        ctlg_name = utils.scope_queue_catalogue(queue,
                                     project, QUEUE_CATALOGUE_SUFFIX)
        pipe.zrem(QUEUE_CATALOGUE_SUFFIX, ctlg_name)
        pipe.delete(ctlg_name)
        pipe.execute()

    @utils.raises_conn_error
    def update(self, project, queue, shards=None):
        ctlg_name = utils.scope_queue_catalogue(queue
                                    , project, QUEUE_CATALOGUE_SUFFIX)
        self._client.set(ctlg_name, shards)

    @utils.raises_conn_error
    @utils.reset_pipeline
    def drop_all(self):
        pipe = self._pipeline
        # Retrieve the list of all ctlg entries.
        # NOTE(prashanthr_): Not being used from the ctlg
        ctlg_entries = self._client.zrange(QUEUE_CATALOGUE_SUFFIX, 0, -1)

        pipe.zremrangebyscore(QUEUE_CATALOGUE_SUFFIX, 0, -1)

        for ctlg_entry in ctlg_entries:
            pipe.delete(ctlg_entry)

        pipe.execute()


def _normalize(project, queue, shard):
    return {
        "project": project,
        "queue": queue,
        "shard": shard
    }
