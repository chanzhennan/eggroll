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
from collections import Iterable

import grpc
from concurrent import futures

import numpy as np

from eggroll.core.command.command_router import CommandRouter
from eggroll.core.command.command_service import CommandServicer
from eggroll.core.datastructure.broker import FifoBroker
from eggroll.core.io.kv_adapter import RocksdbSortedKvAdapter, LmdbSortedKvAdapter
from eggroll.core.io.io_utils import get_db_path
from eggroll.core.meta_model import ErTask, ErPartition
from eggroll.core.proto import command_pb2_grpc, transfer_pb2_grpc
from eggroll.core.serdes import cloudpickle
from eggroll.core.serdes import eggroll_serdes
from eggroll.core.transfer.transfer_service import GrpcTransferServicer, \
  TransferClient
from eggroll.roll_pair.shuffler import DefaultShuffler
from grpc._cython import cygrpc

def generator(serde, iterator):
  for k, v in iterator:
    print("yield ({}, {})".format(serde.deserialize(k), serde.deserialize(v)))
    yield serde.deserialize(k), serde.deserialize(v)

class EggPair(object):
  uri_prefix = 'v1/egg-pair'
  GET = "get"
  PUT = "put"

  def __init__(self):
    self.serde = self._create_serde()

  def get_unary_input_adapter(self, task_info: ErTask):
    input_partition = task_info._inputs[0]
    print("input_adapter: ", input_partition, "path: ",
          get_db_path(input_partition))
    input_adapter = None
    if task_info._inputs[0]._store_locator._store_type == "rollpair.lmdb":
      input_adapter = LmdbSortedKvAdapter(
        options={'path': get_db_path(partition=input_partition)})
    elif task_info._inputs[0]._store_locator._store_type == "rollpair.leveldb":
      input_adapter = RocksdbSortedKvAdapter(
        options={'path': get_db_path(partition=input_partition)})
    return input_adapter

  def get_binary_input_adapter(self, task_info: ErTask):
    left_partition = task_info._inputs[0]
    right_partition = task_info._inputs[1]
    print("left partition: ", left_partition, "path: ",
          get_db_path(left_partition))
    print("right partition: ", right_partition, "path: ",
          get_db_path(right_partition))
    left_adapter = None
    right_adapter = None
    if task_info._inputs[0]._store_locator._store_type == "rollpair.lmdb":
      left_adapter = LmdbSortedKvAdapter(
        options={'path': get_db_path(left_partition)})
      right_adapter = LmdbSortedKvAdapter(
        options={'path': get_db_path(right_partition)})
    elif task_info._inputs[0]._store_locator._store_type == "rollpair.leveldb":
      left_adapter = RocksdbSortedKvAdapter(
        options={'path': get_db_path(left_partition)})
      right_adapter = RocksdbSortedKvAdapter(
        options={'path': get_db_path(right_partition)})

    return left_adapter, right_adapter

  def get_unary_output_adapter(self, task_info: ErTask):
    output_partition = task_info._inputs[0]
    print("output_partition: ", output_partition, "path: ",
          get_db_path(output_partition))
    output_adapter = None
    if task_info._inputs[0]._store_locator._store_type == "rollpair.lmdb":
      output_adapter = LmdbSortedKvAdapter(
        options={'path': get_db_path(partition=output_partition)})
    elif task_info._inputs[0]._store_locator._store_type == "rollpair.leveldb":
      output_adapter = RocksdbSortedKvAdapter(
        options={'path': get_db_path(partition=output_partition)})
    return output_adapter

  def _create_serde(self):
    return eggroll_serdes.get_serdes()

  def run_task(self, task: ErTask):
    functors = task._job._functors
    result = task

    if task._name == 'get':
      print("egg_pair get call")
      input_adapter = self.get_unary_output_adapter(task_info=task)
      input_iterator = input_adapter.get(b'test_get_key')

    if task._name == 'put':
      print("egg_pair put call")
      input_adapter = self.get_unary_output_adapter(task_info=task)
      input_iterator = input_adapter.put(b'test_put_key', b'test_put_val')

    if task._name == 'mapValues':
      f = cloudpickle.loads(functors[0]._body)
      input_adapter = self.get_unary_input_adapter(task_info=task)
      output_adapter = self.get_unary_output_adapter(task_info=task)
      input_iterator = input_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      for k_bytes, v_bytes in input_iterator:
        v = self.serde.deserialize(v_bytes)
        output_writebatch.put(k_bytes, self.serde.serialize(f(v)))

      output_writebatch.close()
      input_adapter.close()
      output_adapter.close()

    elif task._name == 'map':
      f = cloudpickle.loads(functors[0]._body)
      p = cloudpickle.loads(functors[1]._body)

      input_adapter = self.get_unary_input_adapter(task_info=task)
      output_partition = task._outputs[0]
      print("output partition: ", output_partition, "path: ",
            get_db_path(output_partition))
      output_store = task._job._outputs[0]

      shuffle_broker = FifoBroker()
      for k_bytes, v_bytes in input_adapter.iteritems():
        shuffle_broker.put(f(k_bytes, v_bytes))
      input_adapter.close()
      shuffle_broker.signal_write_finish()
      shuffler = DefaultShuffler(task._job._id, shuffle_broker, output_store, output_partition, p)
      shuffler.start()
      shuffle_finished = shuffler.wait_until_finished(600)
      print('map finished')

    elif task._name == 'reduce':
      f = cloudpickle.loads(functors[0]._body)

      input_adapter = self.get_unary_input_adapter(task_info=task)
      seq_op_result = None

      for k_bytes, v_bytes in input_adapter.iteritems():
        if seq_op_result:
          seq_op_result = f(seq_op_result, self.serde.deserialize(v_bytes))
        else:
          seq_op_result = self.serde.deserialize(v_bytes)

      partition_id = task._inputs[0]._id
      transfer_tag = task._job._name

      if "0" == partition_id:
        queue = GrpcTransferServicer.get_or_create_broker(transfer_tag)
        partition_size = len(task._job._inputs[0]._partitions)

        comb_op_result = seq_op_result

        for i in range(1, partition_size):
          other_seq_op_result = queue.get(block=True, timeout=10)

          comb_op_result = f(comb_op_result, other_seq_op_result)

        output_adapter = self.get_unary_output_adapter(task_info=task)
        output_writebatch = output_adapter.new_batch()
        output_writebatch.put('result'.encode(), self.serde.serialize(comb_op_result))

        output_writebatch.close()
        output_adapter.close()
      else:
        transfer_client = TransferClient()
        transfer_client.send(data=seq_op_result, tag=transfer_tag,
                             server_node=task._outputs[0]._processor)

      input_adapter.close()

    elif task._name == 'mapPartitions':
      f = cloudpickle.loads(functors[0]._body)
      input_adapter = self.get_unary_input_adapter(task_info=task)
      output_adapter = self.get_unary_output_adapter(task_info=task)
      input_iterator = input_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      value = f(generator(self.serde, input_iterator))
      if input_iterator.last():
        print("value of mapPartitions2:{}".format(value))
        if isinstance(value, Iterable):
          for k1, v1 in value:
            output_writebatch.put(self.serde.serialize(k1), self.serde.serialize(v1))
        else:
          key = input_iterator.key()
          output_writebatch.put(key, self.serde.serialize(value))

      output_writebatch.close()
      output_adapter.close()
      input_adapter.close()

    elif task._name == 'collapsePartitions':
      f = cloudpickle.loads(functors[0]._body)
      input_adapter = self.get_unary_input_adapter(task_info=task)
      output_adapter = self.get_unary_output_adapter(task_info=task)
      input_iterator = input_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      value = f(generator(self.serde, input_iterator))
      if input_iterator.last():
        key = input_iterator.key()
        output_writebatch.put(key, self.serde.serialize(value))

      output_writebatch.close()
      output_adapter.close()
      input_adapter.close()

    elif task._name == 'flatMap':
      f = cloudpickle.loads(functors[0]._body)
      input_adapter = self.get_unary_input_adapter(task_info=task)
      output_adapter = self.get_unary_output_adapter(task_info=task)
      input_iterator = input_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      for k1, v1 in input_iterator:
        for k2, v2 in f(self.serde.deserialize(k1), self.serde.deserialize(v1)):
          output_writebatch.put(self.serde.serialize(k2), self.serde.serialize(v2))

      output_writebatch.close()
      output_adapter.close()
      input_adapter.close()

    elif task._name == 'glom':
      input_adapter = self.get_unary_input_adapter(task_info=task)
      output_adapter = self.get_unary_output_adapter(task_info=task)
      input_iterator = input_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      k_tmp = None
      v_list = []
      for k, v in input_iterator:
        v_list.append((self.serde.deserialize(k), self.serde.deserialize(v)))
        k_tmp = k
      if k_tmp is not None:
        output_writebatch.put(k_tmp, self.serde.serialize(v_list))

      output_writebatch.close()
      output_adapter.close()
      input_adapter.close()

    elif task._name == 'sample':
      fraction = cloudpickle.loads(functors[0]._body)
      seed = cloudpickle.loads(functors[1]._body)
      input_adapter = self.get_unary_input_adapter(task_info=task)
      output_adapter = self.get_unary_output_adapter(task_info=task)
      input_iterator = input_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      input_iterator.first()
      random_state = np.random.RandomState(seed)
      for k, v in input_iterator:
        if random_state.rand() < fraction:
          output_writebatch.put(k, v)

      output_writebatch.close()
      output_adapter.close()
      input_adapter.close()

    elif task._name == 'filter':
      f = cloudpickle.loads(functors[0]._body)
      input_adapter = self.get_unary_input_adapter(task_info=task)
      output_adapter = self.get_unary_output_adapter(task_info=task)
      input_iterator = input_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      for k ,v in input_iterator:
        if f(self.serde.deserialize(k), self.serde.deserialize(v)):
          output_writebatch.put(k, v)

      output_writebatch.close()
      output_adapter.close()
      input_adapter.close()

    elif task._name == 'subtractByKey':
      left_adapter, right_adapter = self.get_binary_input_adapter(task_info=task)
      output_adapter = self.get_unary_output_adapter(task_info=task)
      left_iterator = left_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      for k_left, v_left in left_iterator:
        v_right = right_adapter.get(k_left)
        if v_right is None:
          output_writebatch.put(k_left, v_left)

      output_writebatch.close()
      output_adapter.close()
      left_adapter.close()
      right_adapter.close()

    elif task._name == 'union':
      f = cloudpickle.loads(functors[0]._body)
      left_adapter, right_adapter = self.get_binary_input_adapter(task_info=task)
      output_adapter = self.get_unary_output_adapter(task_info=task)
      left_iterator = left_adapter.iteritems()
      right_iterator = right_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      #store the iterator that has been iterated before
      k_list_iterated = []

      for k_left, v_left in left_iterator:
        v_right = right_adapter.get(k_left)
        if v_right is None:
          output_writebatch.put(k_left, v_left)
        else:
          k_list_iterated.append(self.serde.deserialize(v_left))
          v_final = f(v_left, v_right)
          output_writebatch.put(k_left, v_final)

      for k_right, v_right in right_iterator:
        if self.serde.deserialize(k_right) not in k_list_iterated:
          output_writebatch.put(k_right, v_right)

      output_writebatch.close()
      output_adapter.close()
      left_adapter.close()
      right_adapter.close()

    elif task._name == 'join':
      f = cloudpickle.loads(functors[0]._body)
      left_adapter, right_adapter = self.get_binary_input_adapter(task_info=task)
      output_adapter = self.get_unary_output_adapter(task_info=task)
      left_iterator = left_adapter.iteritems()
      right_iterator = right_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      for k_bytes, l_v_bytes in left_iterator:
        r_v_bytes = right_adapter.get(k_bytes)
        if r_v_bytes:
          output_writebatch.put(k_bytes,
                                self.serde.serialize(f(self.serde.deserialize(l_v_bytes),
                                                       self.serde.deserialize(r_v_bytes))))

      output_writebatch.close()
      output_adapter.close()
      left_adapter.close()
      right_adapter.close()

    print('result: ', result)
    return result


def serve():
  port = 20001
  prefix = 'v1/egg-pair'

  #storage api
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/get",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/put",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")

  #computing api
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/mapValues",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
      service_name=f"{prefix}/map",
      route_to_module_name="eggroll.roll_pair.egg_pair",
      route_to_class_name="EggPair",
      route_to_method_name="run_task")
  CommandRouter.get_instance().register(
      service_name=f"{prefix}/reduce",
      route_to_module_name="eggroll.roll_pair.egg_pair",
      route_to_class_name="EggPair",
      route_to_method_name="run_task")
  CommandRouter.get_instance().register(
      service_name=f"{prefix}/join",
      route_to_module_name="eggroll.roll_pair.egg_pair",
      route_to_class_name="EggPair",
      route_to_method_name="run_task")
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/mapPartitions",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/collapsePartitions",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/flatMap",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/glom",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/sample",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/filter",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/subtractByKey",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")
  CommandRouter.get_instance().register(
    service_name=f"{prefix}/union",
    route_to_module_name="eggroll.roll_pair.egg_pair",
    route_to_class_name="EggPair",
    route_to_method_name="run_task")

  server = grpc.server(futures.ThreadPoolExecutor(max_workers=5),
                       options=[
                         (cygrpc.ChannelArgKey.max_send_message_length, -1),
                         (cygrpc.ChannelArgKey.max_receive_message_length, -1)])

  command_servicer = CommandServicer()
  # todo: register egg_pair methods
  command_pb2_grpc.add_CommandServiceServicer_to_server(command_servicer,
                                                        server)

  transfer_servicer = GrpcTransferServicer()
  transfer_pb2_grpc.add_TransferServiceServicer_to_server(transfer_servicer,
                                                          server)

  server.add_insecure_port(f'[::]:{port}')

  server.start()

  print('server started')
  import time
  time.sleep(10000)


if __name__ == '__main__':
  serve()
