# Copyright (c) 2013 Rackspace, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License.  You may obtain a copy
# of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import uuid

from oslo.config import cfg

from marconi.openstack.common.cache import cache as oslo_cache
from marconi.queues.storage import pooling
from marconi.queues.storage import sqlalchemy
from marconi.queues.storage import utils
from marconi import tests as testing


# TODO(cpp-cabrera): it would be wonderful to refactor this unit test
# so that it could use multiple control storage backends once those
# have pools/catalogue implementations.
@testing.requires_mongodb
class PoolCatalogTest(testing.TestBase):

    config_file = 'wsgi_mongodb_pooled.conf'

    def setUp(self):
        super(PoolCatalogTest, self).setUp()

        self.conf.register_opts([cfg.StrOpt('storage')],
                                group='drivers')
        cache = oslo_cache.get_cache()
        control = utils.load_storage_driver(self.conf, cache,
                                            control_mode=True)

        self.catalogue_ctrl = control.catalogue_controller
        self.pools_ctrl = control.pools_controller

        # NOTE(cpp-cabrera): populate catalogue
        self.pool = str(uuid.uuid1())
        self.queue = str(uuid.uuid1())
        self.project = str(uuid.uuid1())
        self.pools_ctrl.create(self.pool, 100, 'sqlite://:memory:')
        self.catalogue_ctrl.insert(self.project, self.queue, self.pool)
        self.catalog = pooling.Catalog(self.conf, cache, control)

    def tearDown(self):
        self.catalogue_ctrl.drop_all()
        self.pools_ctrl.drop_all()
        super(PoolCatalogTest, self).tearDown()

    def test_lookup_loads_correct_driver(self):
        storage = self.catalog.lookup(self.queue, self.project)
        self.assertIsInstance(storage, sqlalchemy.DataDriver)

    def test_lookup_returns_none_if_queue_not_mapped(self):
        self.assertIsNone(self.catalog.lookup('not', 'mapped'))

    def test_lookup_returns_none_if_entry_deregistered(self):
        self.catalog.deregister(self.queue, self.project)
        self.assertIsNone(self.catalog.lookup(self.queue, self.project))

    def test_register_leads_to_successful_lookup(self):
        self.catalog.register('not_yet', 'mapped')
        storage = self.catalog.lookup('not_yet', 'mapped')
        self.assertIsInstance(storage, sqlalchemy.DataDriver)
