# Copyright (c) 2013 Red Hat, Inc.
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

"""This module implements a common Pipeline object.

The pipeline can be used to enhance the storage layer with filtering, routing,
multiplexing and the like. For example:

    >>> stages = [MessageFilter(), EncryptionFilter(), QueueController()]
    >>> pipeline = Pipeline(stages)

Every stage has to implement the method it wants to hook into. This method
will be called when the pipeline consumption gets to that point - stage
ordering matters - and will continue unless the method call returns a value
that is not None.

At least one of the stages has to implement the calling method. If none of
them do, an AttributeError exception will be raised.
"""

import functools

import six

import marconi.openstack.common.log as logging

LOG = logging.getLogger(__name__)


class Pipeline(object):

    def __init__(self, pipeline=None):
        self._pipeline = pipeline and list(pipeline) or []

    def append(self, stage):
        self._pipeline.append(stage)

    def __getattr__(self, name):
        return functools.partial(self.consume_for, name)

    def consume_for(self, method, *args, **kwargs):
        """Consumes the pipeline for `method`.

        This method walks through the pipeline and calls
        `method` for each of the items in the pipeline. A
        warning will be logged for each pipe not implementing
        `method` and an Attribute error will be raised if
        none of the stages do.

        :params method: The method name to call on each pipe
        :type method: `six.text_type`
        :param args: Positional arguments to pass to the call.
        :param kwargs: Keyword arguments to pass to the call.

        :returns: Anything returned by the called methods.
        :raises: AttributeError if none of the stages implement `method`
        """
        # NOTE(flaper87): Used as a way to verify
        # the requested method exists in at least
        # one of the stages, otherwise AttributeError
        # will be raised.
        target = None

        for stage in self._pipeline:
            try:
                target = getattr(stage, method)
            except AttributeError:
                msg = _(u'Stage {0} does not implement {1}')
                LOG.warning(msg.format(six.text_type(stage), method))
                continue

            result = target(*args, **kwargs)

            # NOTE(flaper87): Will keep going forward
            # through the pipeline unless the call returns
            # something.
            if result is not None:
                return result

        if target is None:
            msg = _(u'Method {0} not found in any of '
                    'the registered stages').format(method)
            LOG.error(msg)
            raise AttributeError(msg)
