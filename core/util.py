

def must_one(arr):
    arr = set(arr)
    if len(arr) == 0:
        raise ValueError("Must one but is empty")
    if len(arr) > 1:
        raise ValueError("Must one but is more: "+", ".join(sorted(arr)))
    val = arr.pop()
    if val is None:
        raise ValueError("Must one but is None")
    return val


def read_file(file: str, *args, **kwargs):
    with open(file, "r") as f:
        txt = f.read().strip()
        txt = txt.format(*args, **kwargs)
        return txt
