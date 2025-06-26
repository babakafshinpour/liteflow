
**Liteflow** is a lightweight, backend-agnostic parallel data processing framework that mimics a Spark-style API. It allows seamless switching between local execution and scalable backends like **Dask**, with future support for **Ray** and **Spark**.

## ✨ Features
- Familiar API: `parallelize`, `map`, `filter`, `reduce`, etc.
- Pure Python local backend for debugging and prototyping
- Ray and Dask support for parallel execution
- Pluggable backend architecture
- Easy to extend and integrate with other engines

## 🔧 Installation
```bash
pip install liteflow
pip install "liteflow[ray]"   # for Ray backend
pip install "liteflow[dask]"  # for Dask backend
```

## 🚀 Quickstart
```python
from liteflow import get_context

ctx = get_context("local")  # or "ray", "dask"
rdd = ctx.parallelize([1, 2, 3])
result = rdd.map(lambda x: x * 2).collect()
print(result)  # [2, 4, 6]
```

## 🤝 Contributing
1. Fork the repo
2. Create a branch `git checkout -b feature/xyz`
3. Run tests with `pytest`
4. Submit a pull request

## 📄 License
MIT License

## 🔗 Links
- GitHub: https://github.com/babakafshinpour/liteflow