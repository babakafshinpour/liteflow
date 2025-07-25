import pytest
pytest.importorskip("dask")

from liteflow.backends.dask_backend import DaskContext


def test_dask_basic_map():
    ctx = DaskContext()
    rdd = ctx.parallelize([1, 2, 3])
    result = rdd.map(lambda x: x * 2).collect()
    assert sorted(result) == [2, 4, 6]

def test_dask_filter():
    ctx = DaskContext()
    rdd = ctx.parallelize([1, 2, 3, 4])
    result = rdd.filter(lambda x: x % 2 == 0).collect()
    assert sorted(result) == [2, 4]

def test_dask_reduce():
    ctx = DaskContext()
    rdd = ctx.parallelize([1, 2, 3, 4])
    result = rdd.reduce(lambda x, y: x + y)
    assert result == 10

def test_dask_count():
    ctx = DaskContext()
    rdd = ctx.parallelize([1, 2, 3])
    assert rdd.count() == 3