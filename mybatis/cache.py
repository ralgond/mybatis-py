import json
import pickle
import time
from typing import Dict, Any, Optional

from pympler import asizeof

class CacheKey(object):
    def __init__(self, sql, param_list):
        self.sql = sql
        self.param_list = param_list

class Cache(object):
    def __init__(self, memory_limit:int, max_live_ms:int):
        self.memory_limit = memory_limit
        self.memory_used = 0
        self.max_live_ms = max_live_ms
        self.table : Dict[str, CacheNode] = {}
        self.list = CacheList()

    def empty(self):
        # assert self.list.head.next is self.list.tail
        return len(self.table) == 0

    def clear(self):
        self.table.clear()
        head = self.list.head
        tail = self.list.tail

        head.next = tail
        tail.prev = head

    def put(self, raw_key:CacheKey, value: Any):
        key = json.dumps(raw_key.__dict__)
        if key in self.table:
            node = self.table[key]
            del self.table[key]
            self.list.remove(node)
            self.memory_used -= node.memory_usage
            node.value = json.dumps(value)
            node.memory_usage = asizeof.asizeof(node.key) + asizeof.asizeof(node.value)
            # print("++++>", node.memory_usage)
        else:
            node = CacheNode(key, json.dumps(value))
            node.memory_usage = asizeof.asizeof(node.key) + asizeof.asizeof(node.value)

        # print("====>", node.memory_usage)

        while self.memory_used + node.memory_usage >= self.memory_limit:
            to_remove_node = self.list.tail.prev
            if to_remove_node is not self.list.head:
                del self.table[to_remove_node.key]
                self.list.remove(to_remove_node)
                self.memory_used -= to_remove_node.memory_usage
            else:
                break

        if self.memory_used + node.memory_usage > self.memory_limit:
            return

        self.table[key] = node

        self.list.move_to_head(node)

        self.memory_used += node.memory_usage

    def get(self, raw_key: CacheKey) -> Optional[Any]:
        key = json.dumps(raw_key.__dict__)
        if key not in self.table:
            return None

        node = self.table[key]
        current_time_ms = int(time.time() * 1000)
        if current_time_ms - node.timestamp > self.max_live_ms:
            del self.table[node.key]
            self.list.remove(node)
            self.memory_used -= node.memory_usage
            return None

        self.list.move_to_head(node)
        # print ("====>", node.value, type(node.value))
        return json.loads(node.value)

    # def dump(self):
    #     for node in self.list.traverse():
    #         print(node.key, node.value, node.memory_usage, asizeof.asizeof(node.key) + asizeof.asizeof(node.value))
    def traverse(self):
        node = self.list.head.next
        while node is not self.list.tail:
            yield node.key, json.loads(node.value), node.memory_usage
            node = node.next

class CacheNode:
    def __init__(self, key : Any, value : Any):
        self.prev = None
        self.next = None
        self.memory_usage = asizeof.asizeof(key) + asizeof.asizeof(value)
        self.key = key
        self.value = value
        self.timestamp = int(time.time() * 1000)

class CacheList:
    def __init__(self):
        self.head = CacheNode(None, None)
        self.tail = CacheNode(None, None)
        self.head.next = self.tail
        self.tail.prev = self.head

    @staticmethod
    def remove(node:CacheNode):
        if node.prev is None:
            return

        prev = node.prev
        next = node.next

        prev.next = next
        next.prev = prev

        node.prev = None
        node.next = None

    def insert_after_head(self, node:CacheNode):
        node.next = self.head.next
        self.head.next = node
        node.next.prev = node
        node.prev = self.head

    def move_to_head(self, node:CacheNode):
        CacheList.remove(node)
        self.insert_after_head(node)
