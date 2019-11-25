# -*- coding: utf-8 -*-
#  Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from eggroll.core.command.command_model import ErCommandRequest, ErCommandResponse, CommandURI
from eggroll.core.meta_model import ErStoreLocator, ErJob, ErStore, ErFunctor, ErTask, ErEndpoint, ErPair
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.serdes import cloudpickle
from eggroll.core.command.command_client import CommandClient
from eggroll.cluster_manager.cluster_manager_client import ClusterManagerClient
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.constants import StoreTypes, SerdesTypes, PartitionerTypes
from eggroll.core.serdes import eggroll_serdes
from eggroll.roll_pair.egg_pair import EggPair

class RollPair(object):
  __uri_prefix = 'v1/roll-pair'
  GET = "get"
  PUT = "put"
  MAP = 'map'
  MAP_VALUES = 'mapValues'
  REDUCE = 'reduce'
  JOIN = 'join'
  COLLAPSEPARTITIONS = "collapsePartitions"
  MAPPARTITIONS = "mapPartitions"
  GLOM = "glom"
  FLATMAP = "flatMap"
  SAMPLE = "sample"
  FILTER = "filter"
  SUBTRACTBYKEY = "subtractByKey"
  UNION = "union"
  value_serdes = eggroll_serdes.get_serdes()

  def __init__(self, er_store: ErStore, opts = {'cluster_manager_host': 'localhost', 'cluster_manager_port': 4670}):
    _grpc_channel_factory = GrpcChannelFactory()

    self.__roll_pair_service_endpoint = ErEndpoint(host = opts['roll_pair_service_host'], port = opts['roll_pair_service_port'])
    self.__cluster_manager_channel = _grpc_channel_factory.create_channel(ErEndpoint(opts['cluster_manager_host'], opts['cluster_manager_port']))
    self.__roll_pair_service_channel = _grpc_channel_factory.create_channel(self.__roll_pair_service_endpoint)

    self.__command_serdes = opts.get('serdes', SerdesTypes.PROTOBUF)
    self.__command_client = CommandClient()

    self.__roll_pair_service_stub = command_pb2_grpc.CommandServiceStub(self.__roll_pair_service_channel)
    self.__cluster_manager_client = ClusterManagerClient(opts)
    self._parent_opts = opts

    # todo: integrate with session mechanism
    self.__seq = 1
    self.__session_id = '1'

    self.land(er_store, opts)

  def __repr__(self):
    return f'python RollPair(_store={self.__store})'

  def land(self, er_store: ErStore, opts = {}):
    if er_store:
      final_store = er_store
    else:
      store_type = opts.get('store_type', StoreTypes.ROLLPAIR_LEVELDB)
      namespace = opts.get('namespace', '')
      name = opts.get('name', '')
      total_partiitons = opts.get('total_partitions', 0)
      partitioner = opts.get('partitioner', PartitionerTypes.BYTESTRING_HASH)
      serdes = opts.get('serdes', SerdesTypes.PICKLE)

      final_store = ErStore(
          store_locator = ErStoreLocator(
              store_type = store_type,
              namespace = namespace,
              name = name,
              total_partiitons = total_partiitons,
              partitioner = partitioner,
              serdes = serdes))

    self.__store = self.__cluster_manager_client.get_or_create_store(final_store)

  def __get_seq(self):
    self.__seq = self.__seq + 1
    return self.__seq

  """
  
    storage api
  
  """
  def get(self, key, opt = {}):
    er_pair = ErPair(key=key, value=None)
    outputs = []
    job = ErJob(id=self.__session_id, name=RollPair.GET,
                inputs=[self.__store],
                outputs=outputs,
                functors=[ErFunctor(body=er_pair.to_proto_string())])
    job_request = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{EggPair.uri_prefix}/{EggPair.GET}'),
      serdes_type=self.__command_serdes
    )
    er_store = job_request._outputs[0]
    print(er_store)
    return RollPair(er_store, opts=self._parent_opts)

  def put(self, key, value, opt = {}):
    er_pair = ErPair(key=key, value=value)
    outputs = []
    job = ErJob(id=self.__session_id, name=RollPair.GET,
                inputs=[self.__store],
                outputs=outputs,
                functors=[ErFunctor(body=er_pair.to_proto_string())])
    job_request = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{EggPair.uri_prefix}/{EggPair.GET}'),
      serdes_type=self.__command_serdes
    )
    er_store = job_request._outputs[0]
    print(er_store)
    return RollPair(er_store, opts=self._parent_opts)

  """
  
   computing api
  
  """
  def map_values(self, func, output = None, opt = {}):
    functor = ErFunctor(name=RollPair.MAP_VALUES, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)
    job = ErJob(id=self.__session_id, name=RollPair.MAP_VALUES,
                inputs=[self.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__command_client.simple_sync_send(
        input=job,
        output_type=ErJob,
        endpoint=self.__roll_pair_service_endpoint,
        command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.MAP_VALUES}'),
        serdes_type=self.__command_serdes)

    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, opts=self._parent_opts)

  def map_partitions(self, func, output = None, opt = {}):
    functor = ErFunctor(name=RollPair.MAPPARTITIONS, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)
    job  = ErJob(id=self.__session_id, name=RollPair.MAPPARTITIONS,
                 inputs=[self.__store],
                 outputs=outputs,
                 functors=[functor])

    job_result = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.MAPPARTITIONS}'),
      serdes_type=self.__command_serdes
    )
    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, opts=self._parent_opts)

  def collapse_partitions(self, func, output = None, opt = {}):
    functor = ErFunctor(name=RollPair.COLLAPSEPARTITIONS, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)

    job = ErJob(id=self.__session_id, name=RollPair.COLLAPSEPARTITIONS,
                inputs=[self.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.COLLAPSEPARTITIONS}'),
      serdes_type=self.__command_serdes
    )
    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, opts=self._parent_opts)

  def flat_map(self, func, output=None, opt={}):
    functor = ErFunctor(name=RollPair.FLATMAP, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)

    job = ErJob(id=self.__session_id, name=RollPair.FLATMAP,
                inputs=[self.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.FLATMAP}'),
      serdes_type=self.__command_serdes
    )
    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, opts=self._parent_opts)

  def map(self, func, partition_func, output=None, opt={}):
    functor = ErFunctor(name=RollPair.MAP, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    partitioner = ErFunctor(name=RollPair.MAP, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(partition_func))
    outputs = []
    if output:
      outputs.append(output)

    job = ErJob(id=self.__session_id, name=RollPair.MAP,
                inputs=[self.__store],
                outputs=outputs,
                functors=[functor, partitioner])

    job_result = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.MAP}'),
      serdes_type=self.__command_serdes
    )
    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, opts=self._parent_opts)

  def reduce(self, func, output=None, opt={}):
    functor = ErFunctor(name=RollPair.REDUCE, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)

    job = ErJob(id=self.__session_id, name=RollPair.REDUCE,
                inputs=[self.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.REDUCE}'),
      serdes_type=self.__command_serdes
    )
    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, opts=self._parent_opts)

  def glom(self, output=None, opt={}):
    functor = ErFunctor(name=RollPair.GLOM, serdes=SerdesTypes.CLOUD_PICKLE)
    outputs = []
    if output:
      outputs.append(output)

    job = ErJob(id=self.__session_id, name=RollPair.GLOM,
                inputs=[self.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.GLOM}'),
      serdes_type=self.__command_serdes
    )
    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, opts=self._parent_opts)

  def sample(self, fraction, seed=None, output=None, opt={}):
    er_fraction = ErFunctor(name=RollPair.REDUCE, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(fraction))
    er_seed  = ErFunctor(name=RollPair.REDUCE, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(seed))

    outputs = []
    if output:
      outputs.append(output)
    job = ErJob(id=self.__session_id, name=RollPair.SAMPLE,
                inputs=[self.__store],
                outputs=outputs,
                functors=[er_fraction, er_seed])

    job_result = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.SAMPLE}'),
      serdes_type=self.__command_serdes)

    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, opts=self._parent_opts)

  def filter(self, func, output=None, opt={}):
    functor = ErFunctor(name=RollPair.FILTER, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))

    outputs = []
    if output:
      outputs.append(output)
    job = ErJob(id=self.__session_id, name=RollPair.FILTER,
                inputs=[self.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.FILTER}'),
      serdes_type=self.__command_serdes)

    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, opts=self._parent_opts)

  def subtract_by_key(self, other, output=None, opt={}):
    functor = ErFunctor(name=RollPair.SUBTRACTBYKEY, serdes=SerdesTypes.CLOUD_PICKLE)
    outputs = []
    if output:
      outputs.append(output)
    job = ErJob(id=self.__session_id, name=RollPair.SUBTRACTBYKEY,
                inputs=[self.__store, other.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.SUBTRACTBYKEY}'),
      serdes_type=self.__command_serdes)
    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, opts=self._parent_opts)

  def union(self, other, func=lambda v1, v2: v1, output=None, opt={}):
    functor = ErFunctor(name=RollPair.SUBTRACTBYKEY, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)
    job = ErJob(id=self.__session_id, name=RollPair.SUBTRACTBYKEY,
                inputs=[self.__store, other.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.SUBTRACTBYKEY}'),
      serdes_type=self.__command_serdes)
    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, opts=self._parent_opts)

  def join(self, other, func, output=None, opt={}):
    functor = ErFunctor(name=RollPair.JOIN, serdes=SerdesTypes.CLOUD_PICKLE, body=cloudpickle.dumps(func))
    outputs = []
    if output:
      outputs.append(output)
    job = ErJob(id=self.__session_id, name=RollPair.JOIN,
                inputs=[self.__store, other.__store],
                outputs=outputs,
                functors=[functor])

    job_result = self.__command_client.simple_sync_send(
      input=job,
      output_type=ErJob,
      endpoint=self.__roll_pair_service_endpoint,
      command_uri=CommandURI(f'{RollPair.__uri_prefix}/{RollPair.JOIN}'),
      serdes_type=self.__command_serdes)
    er_store = job_result._outputs[0]
    print(er_store)

    return RollPair(er_store, opts=self._parent_opts)
