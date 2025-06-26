from liteflow.backends.local_backend import LocalContext

def test_local_basic_map():
    ctx = LocalContext()
    rdd = ctx.parallelize([1, 2, 3])
    result = rdd.map(lambda x: x * 2).collect()
    assert result == [2, 4, 6]

def test_local_filter():
    ctx = LocalContext()
    rdd = ctx.parallelize([1, 2, 3, 4])
    result = rdd.filter(lambda x: x % 2 == 0).collect()
    assert result == [2, 4]

def test_local_reduce():
    ctx = LocalContext()
    rdd = ctx.parallelize([1, 2, 3, 4])
    result = rdd.reduce(lambda x, y: x + y)
    assert result == 10

def test_local_count():
    ctx = LocalContext()
    rdd = ctx.parallelize([1, 2, 3])
    assert rdd.count() == 3