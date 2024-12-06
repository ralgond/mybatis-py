import time

import pytest
from mybatis import Cache, CacheKey

def test_basic():
    cache = Cache(memory_limit=555, max_live_ms=10*1000)  # 50MB, 10sec
    cache.put(CacheKey("a", [1, 'a', None]), [{"a1": 1}, {"a2": 2}])
    cache.put(CacheKey("b", [2, 'b', None]), "2")
    cache.put(CacheKey("c", [3, 'c', None]), "3")
    cache.put(CacheKey("d", [4, 'd', None]), None)

    assert cache.get(CacheKey('a', [1, 'a', None])) == None
    assert cache.get(CacheKey('b', [2, 'b', None])) == '2'

    l = []
    for key, value, memory_usage in cache.traverse():
        l.append((key, value, memory_usage, type(value)))

    assert len(l) == 3
    assert l[0][0] == '{"sql": "b", "param_list": [2, "b", null]}'
    assert l[0][1] == '2'

    assert l[1][0] == '{"sql": "d", "param_list": [4, "d", null]}'
    assert l[1][1] == None

    assert l[2][0] == '{"sql": "c", "param_list": [3, "c", null]}'
    assert l[2][1] == '3'

    cache.put(CacheKey("e", [5, 'e', None]), "5")


    l = []
    for key, value, memory_usage in cache.traverse():
        l.append((key, value, memory_usage, type(value)))

    assert len(l) == 3

    assert l[0][0] == '{"sql": "e", "param_list": [5, "e", null]}'
    assert l[0][1] == '5'

    assert l[1][0] == '{"sql": "b", "param_list": [2, "b", null]}'
    assert l[1][1] == '2'

    assert l[2][0] == '{"sql": "d", "param_list": [4, "d", null]}'
    assert l[2][1] == None

def test_timeout():
    cache = Cache(memory_limit=555, max_live_ms=1 * 1000)  # 50MB, 10sec
    cache.put(CacheKey("a", [1, 'a', None]), [{"a1": 1}, {"a2": 2}])
    cache.put(CacheKey("b", [2, 'b', None]), "2")
    cache.put(CacheKey("c", [3, 'c', None]), "3")
    cache.put(CacheKey("d", [4, 'd', None]), None)

    time.sleep(2)
    assert cache.get(CacheKey("a", [1, 'a', None])) == None
    assert cache.get(CacheKey("b", [2, 'b', None])) == None
    assert cache.get(CacheKey("c", [3, 'c', None])) == None
    assert cache.get(CacheKey("d", [4, 'd', None])) == None

    assert cache.memory_used == 0

def test_overwrite():
    cache = Cache(memory_limit=180, max_live_ms=10 * 1000)  # 50MB, 10sec
    cache.put(CacheKey("a", [1, 'a', None]), [{"a1": 1}, {"a2": 2}])
    #print ("++++>cache.memory_used:", cache.memory_used)
    cache.put(CacheKey("a", [1, 'a', None]), [{"a1": 1}, {"a2": 2}, {"a3":3}])
    #print("++++>cache.memory_used:", cache.memory_used)
    assert cache.get(CacheKey("a", [1, 'a', None])) == None
    assert cache.memory_used == 0

    cache = Cache(memory_limit=2000, max_live_ms=10 * 1000)  # 50MB, 10sec
    cache.put(CacheKey("a", [1, 'a', None]), [{"a1": 1}, {"a2": 2}])
    cache.put(CacheKey("a", [1, 'a', None]), [{"a1": 1}, {"a2": 2}, {"a3": 3}])
    assert cache.get(CacheKey("a", [1, 'a', None])) != None

    #print("====>", cache.memory_used)
