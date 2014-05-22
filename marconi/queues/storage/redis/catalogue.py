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

from marconi.queues.storage import base

class CatalogueController(base.CatalogueBase):

    def __init__(self, *args, **kwargs):
        super(CatalogueController, self).__init__(*args, **kwargs)

    
    def list(self, project):
        """Returns a list of queue entries from the catalogue associated with
        this project.

        :param project: The project to use when filtering through queue
                        entries.
        :type project: six.text_type
        :returns: [{'project': ..., 'queue': ..., 'shard': ...},]
        :rtype: [dict]
        """
        raise NotImplementedError

    
    def get(self, project, queue):
        """Returns the shard identifier for the queue registered under this
        project.

        :param project: Namespace to search for the given queue
        :type project: six.text_type
        :param queue: The name of the queue to search for
        :type queue: six.text_type
        :returns: {'shard': ...}
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

    
    def insert(self, project, queue, shard):
        """Creates a new catalogue entry, or updates it if it already existed.

        :param project: str - Namespace to insert the given queue into
        :type project: six.text_type
        :param queue: str - The name of the queue to insert
        :type queue: six.text_type
        :param shard: shard identifier to associate this queue with
        :type shard: six.text_type
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

    
    def update(self, project, queue, shards=None):
        """Updates the shard identifier for this queue

        :param project: Namespace to search
        :type project: six.text_type
        :param queue: The name of the queue
        :type queue: six.text_type
        :param shards: The name of the shard where this project/queue lives.
        :type shards: six.text_type
        :raises: QueueNotMapped
        """
        raise NotImplementedError

    
    def drop_all(self):
        """Drops all catalogue entries from storage."""
        raise NotImplementedError