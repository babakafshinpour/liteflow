
def get_context(backend: str = "local", **kwargs):
    if backend == "local":
        from ..backends.local_backend import LocalContext
        return LocalContext(**kwargs)
    elif backend == "ray":
        from ..backends.ray_backend import RayContext
        return RayContext(**kwargs)
    else:
        raise ValueError(f"Unsupported backend: {backend}")
