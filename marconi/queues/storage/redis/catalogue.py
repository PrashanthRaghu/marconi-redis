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
"""Redis storage controller for the queues ctlg.

Serves to construct an association between a project + queue -> pool

    Tracked pool ctlgs:
    ------------------------
    Redis id: (ctlg)

    Redis Data Structure:
    ---------------------
    Id                                     Value
    ---------------------------------------------
    'project_queue_catalogue'   ->         'pool'
"""
from marconi.queues.storage import base

QUEUE_CATALOGUE_SUFFIX = 'catalogue'


class CatalogueController(base.CatalogueBase):

    def list(self, project):
        """Get a list of queues from the catalogue.

        :param project: The project to use when filtering through queue
                        entries.
        :type project: six.text_type
        :returns: [{'project': ..., 'queue': ..., 'pool': ...},]
        :rtype: [dict]
        """
        raise NotImplementedError

    def get(self, project, queue):
        """Returns the pool identifier for the given queue.

        :param project: Namespace to search for the given queue
        :type project: six.text_type
        :param queue: The name of the queue to search for
        :type queue: six.text_type
        :returns: {'pool': ...}
        :rtype: dict
        :raises: QueueNotMapped
        """
        raise NotImplementedError

    def exists(self, project, queue):
        """Determines whether the given queue exists under project.

        :param project: Namespace to check.
        :type project: six.text_type
        :param queue: str - Particular queue to check for
        :type queue: six.text_type
        :return: True if the queue exists under this project
        :rtype: bool
        """
        raise NotImplementedError

    def insert(self, project, queue, pool):
        """Creates a new catalogue entry, or updates it if it already existed.

        :param project: str - Namespace to insert the given queue into
        :type project: six.text_type
        :param queue: str - The name of the queue to insert
        :type queue: six.text_type
        :param pool: pool identifier to associate this queue with
        :type pool: six.text_type
        """
        raise NotImplementedError

    def delete(self, project, queue):
        """Removes this entry from the catalogue.

        :param project: The namespace to search for this queue
        :type project: six.text_type
        :param queue: The queue name to remove
        :type queue: six.text_type
        """
        raise NotImplementedError

    def update(self, project, queue, pools=None):
        """Updates the pool identifier for this queue

        :param project: Namespace to search
        :type project: six.text_type
        :param queue: The name of the queue
        :type queue: six.text_type
        :param pools: The name of the pool where this project/queue lives.
        :type pools: six.text_type
        :raises: QueueNotMapped
        """
        raise NotImplementedError

    def drop_all(self):
        """Drops all catalogue entries from storage."""
        raise NotImplementedError