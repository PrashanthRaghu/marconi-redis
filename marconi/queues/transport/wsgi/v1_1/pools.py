# Copyright (c) 2013 Rackspace Hosting, Inc.
#
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

"""pools: a resource to handle storage pool management

A pool is added by an operator by interacting with the
pooling-related endpoints. When specifying a pool, the
following fields are required:

{
    "name": string,
    "weight": integer,
    "uri": string::uri
}

Furthermore, depending on the underlying storage type of pool being
registered, there is an optional field:

{
    "options": {...}
}
"""

import falcon
import jsonschema

from marconi.common.schemas import pools as schema
from marconi.common.transport.wsgi import utils
from marconi.common import utils as common_utils
from marconi.openstack.common import log
from marconi.queues.storage import errors
from marconi.queues.storage import utils as storage_utils
from marconi.queues.transport import utils as transport_utils
from marconi.queues.transport.wsgi import errors as wsgi_errors

LOG = log.getLogger(__name__)


class Listing(object):
    """A resource to list registered pools

    :param pools_controller: means to interact with storage
    """
    def __init__(self, pools_controller):
        self._ctrl = pools_controller

    def on_get(self, request, response, project_id):
        """Returns a pool listing as objects embedded in an array:

        [
            {"href": "", "weight": 100, "uri": ""},
            ...
        ]

        :returns: HTTP | [200, 204]
        """
        LOG.debug(u'LIST pools')

        store = {}
        request.get_param('marker', store=store)
        request.get_param_as_int('limit', store=store)
        request.get_param_as_bool('detailed', store=store)

        results = {}
        results['pools'] = list(self._ctrl.list(**store))
        for entry in results['pools']:
            entry['href'] = request.path + '/' + entry.pop('name')

        if not results['pools']:
            response.status = falcon.HTTP_204
            return

        response.content_location = request.relative_uri
        response.body = transport_utils.to_json(results)
        response.status = falcon.HTTP_200


class Resource(object):
    """A handler for individual pool.

    :param pools_controller: means to interact with storage
    """
    def __init__(self, pools_controller):
        self._ctrl = pools_controller
        validator_type = jsonschema.Draft4Validator
        self._validators = {
            'weight': validator_type(schema.patch_weight),
            'uri': validator_type(schema.patch_uri),
            'options': validator_type(schema.patch_options),
            'create': validator_type(schema.create)
        }

    def on_get(self, request, response, project_id, pool):
        """Returns a JSON object for a single pool entry:

        {"weight": 100, "uri": "", options: {...}}

        :returns: HTTP | [200, 404]
        """
        LOG.debug(u'GET pool - name: %s', pool)
        data = None
        detailed = request.get_param_as_bool('detailed') or False

        try:
            data = self._ctrl.get(pool, detailed)

        except errors.PoolDoesNotExist as ex:
            LOG.debug(ex)
            raise falcon.HTTPNotFound()

        data['href'] = request.path

        # remove the name entry - it isn't needed on GET
        del data['name']
        response.body = transport_utils.to_json(data)
        response.content_location = request.relative_uri

    def on_put(self, request, response, project_id, pool):
        """Registers a new pool. Expects the following input:

        {"weight": 100, "uri": ""}

        An options object may also be provided.

        :returns: HTTP | [201, 204]
        """
        LOG.debug(u'PUT pool - name: %s', pool)

        data = utils.load(request)
        utils.validate(self._validators['create'], data)
        if not storage_utils.can_connect(data['uri']):
            raise wsgi_errors.HTTPBadRequestBody(
                'cannot connect to %s' % data['uri']
            )
        self._ctrl.create(pool, weight=data['weight'],
                          uri=data['uri'],
                          options=data.get('options', {}))
        response.status = falcon.HTTP_201
        response.location = request.path

    def on_delete(self, request, response, project_id, pool):
        """Deregisters a pool.

        :returns: HTTP | 204
        """
        LOG.debug(u'DELETE pool - name: %s', pool)
        self._ctrl.delete(pool)
        response.status = falcon.HTTP_204

    def on_patch(self, request, response, project_id, pool):
        """Allows one to update a pool's weight, uri, and/or options.

        This method expects the user to submit a JSON object
        containing at least one of: 'uri', 'weight', 'options'. If
        none are found, the request is flagged as bad. There is also
        strict format checking through the use of
        jsonschema. Appropriate errors are returned in each case for
        badly formatted input.

        :returns: HTTP | 200,400
        """
        LOG.debug(u'PATCH pool - name: %s', pool)
        data = utils.load(request)

        EXPECT = ('weight', 'uri', 'options')
        if not any([(field in data) for field in EXPECT]):
            LOG.debug(u'PATCH pool, bad params')
            raise wsgi_errors.HTTPBadRequestBody(
                'One of `uri`, `weight`, or `options` needs '
                'to be specified'
            )

        for field in EXPECT:
            utils.validate(self._validators[field], data)

        if 'uri' in data and not storage_utils.can_connect(data['uri']):
            raise wsgi_errors.HTTPBadRequestBody(
                'cannot connect to %s' % data['uri']
            )
        fields = common_utils.fields(data, EXPECT,
                                     pred=lambda v: v is not None)

        try:
            self._ctrl.update(pool, **fields)
        except errors.PoolDoesNotExist as ex:
            LOG.exception(ex)
            raise falcon.HTTPNotFound()
