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

import grpc
import time
import unittest
from eggroll.core.command.command_model import ErCommandRequest
from eggroll.core.meta_model import ErStoreLocator, ErJob, ErStore, ErFunctor
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.serdes import cloudpickle
from eggroll.roll_pair.roll_pair import RollPair
from eggroll.core.constants import StoreTypes


class TestRollPair(unittest.TestCase):
  opts = {'cluster_manager_host': 'localhost',
          'cluster_manager_port': 4670,
          'roll_pair_service_host': 'localhost',
          'roll_pair_service_port': 20000}

  storage_opts = {'cluster_manager_host': 'localhost',
          'cluster_manager_port': 4670,
          'roll_pair_service_host': 'localhost',
          'roll_pair_service_port': 20001}
  def test_get(self):
    store = ErStore(ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LMDB, namespace="namespace",
                                   name="name"))
    rp = RollPair(store, opts=TestRollPair.storage_opts)
    res = rp.get(bytes('1', encoding='utf-8'))
    print("res: {}".format(res))

  def test_put(self):
    store = ErStore(store_locator=ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LMDB, namespace="namespace",
                                                 name="name"))
    rp = RollPair(er_store=store, opts=TestRollPair.storage_opts)
    res = rp.put(b'key', b'value')
    print("res: {}".format(res))

  def test_map_values(self):
    store = ErStore(ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LMDB, namespace='namespace',
                                   name='name'))
    rp = RollPair(store, opts=TestRollPair.opts)

    res = rp.map_values(lambda v : v + b'~2', output=ErStore(store_locator = ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LMDB, namespace='namespace', name='testMapValues')))
    print("res: {}".format(res))

  def test_reduce(self):
    store = ErStore(ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LMDB, namespace='namespace',
                                   name='name'))
    rp = RollPair(store, opts=TestRollPair.opts)
    res = rp.reduce(lambda x, y : x + y)
    print("res: {}".format(res))

  def test_map_partitions(self):
    store = ErStore(ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LMDB, namespace='namespace',
                                   name='name'))
    rp = RollPair(store, opts=TestRollPair.opts)

    def func(iter):
      ret = []
      for k, v in iter:
        k = int(k)
        v = int(v)
        ret.append((bytes(f"{k}_{v}_0", encoding='utf8'), bytes(str(v ** 2), encoding='utf8')))
        ret.append((bytes(f"{k}_{v}_1", encoding='utf8'), bytes(str(v ** 3), encoding='utf8')))
      return ret
    res = rp.map_partitions(func)
    print("res: {}".format(res))

  def test_collapse_partitions(self):
    store = ErStore(ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LMDB, namespace='namespace',
                                   name='name'))
    rp = RollPair(store, opts=TestRollPair.opts)

    def f(iterator):
      sum = ""
      for k, v in iterator:
        sum += v
      return sum
    res = rp.collapse_partitions(f)
    print("res: {}".format(res))

  def test_filter(self):
    store = ErStore(ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LMDB, namespace='namespace',
                                   name='name'))
    rp = RollPair(store, opts=TestRollPair.opts)
    res = rp.filter(lambda k, v: int(k) % 2 != 0)
    print("res: {}".format(res))

  def test_glom(self):
    store = ErStore(ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LMDB, namespace='namespace',
                                   name='name'))
    rp = RollPair(store, opts=TestRollPair.opts)

    res = rp.glom()
    print("res: {}".format(res))

  def test_flatMap(self):
    store = ErStore(ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LMDB, namespace='namespace',
                                   name='name'))
    rp = RollPair(store, opts=TestRollPair.opts)
    import random
    def foo(k, v):
      k = int(k)
      v = int(v)
      result = []
      r = random.randint(10000, 99999)
      for i in range(0, k):
        result.append((k + r + i, v + r + i))
      return result
    res = rp.flat_map(foo)
    print("res: {}".format(res))

  def test_sample(self):
    store = ErStore(ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LMDB, namespace='namespace',
                                   name='name'))
    rp = RollPair(store, opts=TestRollPair.opts)

    res = rp.sample(0.1, 81)
    print("res: {}".format(res))

  def test_subtractByKey(self):
    left_locator = ErStore(store_locator=ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LMDB, namespace="namespace",
                                                        name='name'))
    right_locator = ErStore(store_locator=ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LMDB, namespace="namespace",
                                                         name='test'))

    left = RollPair(left_locator, opts=TestRollPair.opts)
    right = RollPair(right_locator, opts=TestRollPair.opts)

    res = left.subtract_by_key(right)
    print("res: {}".format(res))

  def test_union(self):
    left_locator = ErStore(store_locator=ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LMDB, namespace="namespace",
                                                        name='name'))
    right_locator = ErStore(store_locator=ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LMDB, namespace="namespace",
                                                         name='test'))

    left = RollPair(left_locator, opts=TestRollPair.opts)
    right = RollPair(right_locator, opts=TestRollPair.opts)

    res = left.union(right, lambda v1, v2: v1 + v2)
    print("res: {}".format(res))

  def test_join(self):
    left_locator = ErStore(store_locator=ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LMDB, namespace="namespace",
                                   name='name'))
    right_locator = ErStore(store_locator=ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LMDB, namespace="namespace",
                                   name='test'))

    left = RollPair(left_locator, opts=TestRollPair.opts)
    right = RollPair(right_locator, opts=TestRollPair.opts)
    res = left.join(right, lambda x, y : x + b' joins ' + y,
                    output=ErStore(store_locator = ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LMDB, namespace='namespace', name='testJoin')))
    print("res: {}".format(res))


  def test_map(self):
    store = ErStore(ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LMDB, namespace='namespace',
                                   name='name'))
    rp = RollPair(store, opts=TestRollPair.opts)

    res = rp.map(lambda k, v: (b'k_' + k, b'v_' + v), lambda k : k[-1] % 4)

    print('res: ', res)

  def test_map_values_raw(self):
    def append_byte(v):
      return v + b'~1'

    pickled_function = cloudpickle.dumps(append_byte)

    store_locator = ErStoreLocator(store_type="levelDb", namespace="ns",
                                   name='name')
    functor = ErFunctor(name="mapValues", body=pickled_function)

    job = ErJob(id="1", name="mapValues",
                inputs=[ErStore(store_locator=store_locator)],
                functors=[functor])

    channel = grpc.insecure_channel(target='localhost:20000',
                                    options=[
                                      ('grpc.max_send_message_length', -1),
                                      ('grpc.max_receive_message_length', -1)])

    roll_pair_stub = command_pb2_grpc.CommandServiceStub(channel)

    request = ErCommandRequest(seq=1,
                               uri='com.webank.eggroll.rollpair.component.RollPair.mapValues',
                               args=[job.to_proto().SerializeToString()])

    print(f"ready to call")
    result = roll_pair_stub.call(request.to_proto())

    print(f"result: {result}")

    time.sleep(1200)

  def test_reduce_raw(self):
    def concat(a, b):
      return a + b

    pickled_function = cloudpickle.dumps(concat)

    store_locator = ErStoreLocator(store_type="levelDb", namespace="ns",
                                   name='name')
    job = ErJob(id="1", name="reduce",
                inputs=[ErStore(store_locator=store_locator)],
                functors=[ErFunctor(name="reduce", body=pickled_function)])

    channel = grpc.insecure_channel(target='localhost:20000',
                                    options=[
                                      ('grpc.max_send_message_length', -1),
                                      ('grpc.max_receive_message_length', -1)])

    roll_pair_stub = command_pb2_grpc.CommandServiceStub(channel)
    request = ErCommandRequest(seq=1,
                               uri='com.webank.eggroll.rollpair.component.RollPair.reduce',
                               args=[job.to_proto().SerializeToString()])

    result = roll_pair_stub.call(request.to_proto())
    time.sleep(1200)


if __name__ == '__main__':
  unittest.main()
