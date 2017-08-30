import os
import pickle
import json
import yaml
import dbm
import pathlib
from functools import wraps, partial


class Holder(type):
    def __new__(cls, name, bases, clsdict):
        clsobj = type.__new__(cls, name, bases, clsdict)
        orig_init = clsobj.__init__
        orig_setattr = clsobj.__setattr__
        clsobj.__setattr__ = object.__setattr__

        @wraps(orig_setattr)
        def __setattr__(self, attr, val):
            if attr in self.__dict__:
                object.__setattr__(self, attr, val)
            else:
                orig_setattr(self, attr, val)

        @wraps(orig_init)
        def __init__(self, *args, **kwargs):
            clsobj.__setattr__ = object.__setattr__
            orig_init(self, *args, **kwargs)
            clsobj.__setattr__ = __setattr__

        clsobj.__init__ = __init__
        return clsobj


class StateSaver(metaclass=Holder):
    def __init__(self, cache_path, erase=False):
        "docstring"
        self.cache_path = pathlib.Path(cache_path)
        self.erase = erase
        self.prep_state()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if not value and self.erase and self.cache_path.exists():
            os.remove(self.cache_path)
        else:
            self.close()


class JState(StateSaver):
    """backup to json"""
    def __init__(self, cache_path, erase=False,
                 load_kwargs=None, dump_kwargs=None):
        self.load_kwargs = load_kwargs or {}
        self.dump_kwargs = dump_kwargs or {}
        super().__init__(cache_path, erase)

    def prep_state(self, load_func=None):
        if not load_func:
            load_func = json.load
        if self.cache_path.exists():
            with self.cache_path.open() as c:
                self.state = load_func(c, **self.load_kwargs)
        else:
            self.state = {}

    def __setattr__(self, attr, val):
        self.state[attr] = val

    def __getattr__(self, attr):
        return self.state[attr]

    def close(self, dump_func=None):
        if not dump_func:
            dump_func = json.dump
        with self.cache_path.open('w') as c:
            dump_func(self.state, c, **self.dump_kwargs)


class DBState(JState):
    def __init__(self, cache_path, erase=False, mode='c', *args, **kwargs):
        self._mode = mode
        super().__init__(cache_path, erase, *args, **kwargs)

    def prep_state(self):
        self.state = dbm.open(self.cache_path.name, self._mode)

    def __setattr__(self, attr, val):
        self.state[attr] = json.dumps(
            val, separators=(':', ','), **self.dump_kwargs)

    def __getattr__(self, attr):
        return json.loads(self.state[attr].decode(), **self.load_kwargs)

    def sync(self):
        self.state.sync()

    def close(self):
        self.state.close()


class YState(JState):
    def prep_state(self):
        super().prep_state(yaml.safe_load)

    def __setattr__(self, attr, val):
        self.state[attr] = val

    def __getattr__(self, attr):
        return self.state[attr]

    def close(self):
        super().close(yaml.safe_dump)


class Looper(JState):
    def __init__(self, cache_path, iterable=None,
                 cache_first=True, safe=True, **kwargs):
        self.safe = safe
        super().__init__(cache_path, **kwargs)
        if safe:
            self.read_cache = None
            self.dump_kwargs.setdefault('separators', (',', ':'))
        if cache_first:
            try:
                self.iterable = iter(self.state['remaining'])
            except KeyError:
                self.iterable = iter(iterable)
        else:
            try:
                self.iterable = iter(iterable)
            except TypeError:
                self.iterable = self.state['remaining']

    def prep_state(self):
        if self.cache_path.exists():
            if self.safe:
                load = partial(json.loads, **self.load_kwargs)
                self.read_cache = self.cache_path.open()
                self.state = load(self.read_cache.readline())
                self.state['remaining'] = map(load, self.read_cache)
            else:
                with self.cache_path.open('rb') as c:
                    self.state = pickle.load(c, **self.load_kwargs)
        else:
            self.state = {}

    def __setattr__(self, attr, val):
        self.state[attr] = val

    def __getattr__(self, attr):
        return self.state[attr]

    def __exit__(self, type, value, traceback):
        if type:
            if self.safe:
                self.safe_dump()
            else:
                self.unsafe_dump()
        else:
            if self.cache_path.exists():
                os.remove(self.cache_path)

    def safe_dump(self):
        if self.read_cache:
            self.read_cache.close()
            del self.state['remaining']
        with self.cache_path.open('w') as cache:
            dump = partial(json.dumps, **self.dump_kwargs)
            print(dump(self.state), file=cache)
            print(*(dump(i) for i in self.iterable), sep='\n', file=cache)

    def unsafe_dump(self):
        with self.cache_path.open('wb') as cache:
            self.state['remaining'] = self.iterable
            pickle.dump(self.state, cache, **self.dump_kwargs)

    def __iter__(self):
        return self.iterable


class FilePos(JState):
    def __init__(self, cache_path, file, *args, **kwargs):
        "docstring"
        self.file = file
        super().__init__(cache_path, *args, **kwargs)
        pos = self.state.get('pos', 0)
        self.file.seek(pos)

    def __exit__(self, type, value, traceback):
        if type:
            self.state['pos'] = self.file.tell()
            self.file.close()

    def __iter__(self):
        return iter(self.file)


def rewind(file):
    pos = file.tell()
    backtrack = 0
    lines = ()
    while len(lines) < 2:
        backtrack += 100
        byteno = pos-backtrack
        if byteno < 0:
            byteno = 0
            backtrack = pos
        file.seek(byteno)
        lines = file.read(backtrack).splitlines()
    newpos = pos-len(lines[-1])-1
    file.seek(newpos)
    return newpos


def state(cache_path, erase=False, dbm_mode=None):
    if dbm_mode:
        return DBState(cache_path, erase, dbm_mode)
    else:
        return JState(cache_path, erase)


def loop(cache_path, iterable=None, cache_first=True, safe=True):
    with Looper(cache_path, iterable, cache_first, safe) as s:
        yield from ((s, i) for i in s)


def iterfile(cache_path, file, erase=True, **kwargs):
    with FilePos(cache_path, file, erase=erase, **kwargs):
        yield from file
