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

import redis

from marconi.common import decorators
from marconi.openstack.common import log as logging
from marconi.queues import storage
from marconi.queues.storage.redis import controllers
from marconi.queues.storage.redis import options
from marconi.queues.storage.redis import utils


LOG = logging.getLogger(__name__)


def _get_redis_client(conf):
    # NOTE(prashanthr_): Check if SSL support is required.
    if conf.use_socket_path:
        return redis.StrictRedis(unix_socket_path=conf.sock_path)
    else:
        host, port = utils.get_hostport(conf['uri'])
        return redis.StrictRedis(host=host, port=port, db=conf.database)


class DataDriver(storage.DataDriverBase):

    def __init__(self, conf, cache):
        super(DataDriver, self).__init__(conf, cache)

        opts = options.REDIS_OPTIONS

        if 'dynamic' in conf:
            names = conf[options.REDIS_GROUP].keys()
            opts = filter(lambda x: x.name not in names, opts)

        self.conf.register_opts(opts,
                                group=options.REDIS_GROUP)
        self.redis_conf = self.conf[options.REDIS_GROUP]

    def is_alive(self):
        try:
            return self.connection.ping()
        except redis.exceptions.ConnectionError:
            return False

    @decorators.lazy_property(write=False)
    def connection(self):
        """Redis client connection instance."""
        return _get_redis_client(self.redis_conf)

    @decorators.lazy_property(write=False)
    def queue_controller(self):
        return controllers.QueueController(self)

    @decorators.lazy_property(write=False)
    def message_controller(self):
        return controllers.MessageController(self)

    @decorators.lazy_property(write=False)
    def claim_controller(self):
        return controllers.ClaimController(self)


class ControlDriver(storage.ControlDriverBase):

    def __init__(self, conf, cache):
        super(ControlDriver, self).__init__(conf, cache)

        self.conf.register_opts(options.REDIS_OPTIONS,
                                group=options.REDIS_GROUP)

        self.redis_conf = self.conf[options.REDIS_GROUP]

    @decorators.lazy_property(write=False)
    def connection(self):
        """Redis client connection instance."""
        return _get_redis_client(self.redis_conf)

    @property
    def pools_controller(self):
        return controllers.PoolsController(self)

    @property
    def catalogue_controller(self):
        return controllers.CatalogueController(self)
