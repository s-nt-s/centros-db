import functools
import os
import time

from .filemanager import FM


class Cache:
    def __init__(self, file: str, *args, reload=False, maxOld=1, **kargs):
        self.file = file
        self.func = None
        self.reload = reload
        self.maxOld = maxOld
        if maxOld is not None:
            self.maxOld = time.time() - (maxOld * 86400)
        self._kargs = kargs

    def parse_file_name(self, *args, **kargv):
        if args or kargv:
            return self.file.format(*args, **kargv)
        return self.file

    def read(self, file, *args, **kargs):
        return FM.load(file, **self._kargs)

    def save(self, file, data, *args, **kargs):
        if data is not None:
            FM.dump(file, data, **self._kargs)

    def tooOld(self, fl):
        if not os.path.isfile(fl):
            return True
        if self.reload:
            return True
        if self.maxOld is None:
            return False
        if os.stat(fl).st_mtime < self.maxOld:
            return True
        return False

    def callCache(self, slf, *args, **kargs):
        fl = self.parse_file_name(*args, **kargs)
        if not self.tooOld(fl):
            data = self.read(fl, *args, **kargs)
            return data
        data = self.func(slf, *args, **kargs)
        self.save(fl, data, *args, **kargs)
        return data

    def __call__(self, func):
        def callCache(*args, **kargs):
            return self.callCache(*args, **kargs)
        functools.update_wrapper(callCache, func)
        self.func = func
        return callCache
