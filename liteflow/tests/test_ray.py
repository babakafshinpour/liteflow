import pytest
from liteflow.backends.ray_backend import RayContext
ray = pytest.importorskip("ray", reason="Ray is not installed")

def test_ray_basic_map():
    ctx = RayContext()
    rdd = ctx.parallelize([1, 2, 3])
    result = rdd.map(lambda x: x * 2).collect()
    assert sorted(result) == [2, 4, 6]

def test_ray_filter():
    ctx = RayContext()
    rdd = ctx.parallelize([1, 2, 3, 4])
    result = rdd.filter(lambda x: x % 2 == 0).collect()
    assert sorted(result) == [2, 4]

def test_ray_reduce():
    ctx = RayContext()
    rdd = ctx.parallelize([1, 2, 3, 4])
    result = rdd.reduce(lambda x, y: x + y)
    assert result == 10

def test_ray_count():
    ctx = RayContext()
    rdd = ctx.parallelize([1, 2, 3])
    assert rdd.count() == 3
