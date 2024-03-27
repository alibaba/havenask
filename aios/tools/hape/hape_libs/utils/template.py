import json
import jinja2
import tempfile
import shutil

class TemporaryDirectory(object):
    def __enter__(self):
        self.name = tempfile.mkdtemp()
        return self.name

    def __exit__(self, exc_type, exc_value, traceback):
        shutil.rmtree(self.name)

class CustomUndefined(jinja2.Undefined):
    def __init__(self, hint=None, obj=None, name=None, exc=None):
        jinja2.Undefined.__init__(self, hint=hint, name=name, exc=exc)
        self._path = [name] if name else []

    def __getattr__(self, name):
        if name == '__bases__':
            raise AttributeError()
        new_obj = CustomUndefined(name=name)
        new_obj._path = self._path + [name]
        return new_obj

    def __unicode__(self):
        path = '.'.join(filter(None, self._path))
        return "{{ %s }}" % path
    
    def __call__(self, *args, **kwargs):
        path = '.'.join(filter(None, self._path))
        args_repr = [repr(arg) for arg in args]
        kwargs_repr = ["{}={}".format(key, repr(value)) for key, value in kwargs.items()]
        params = ", ".join(args_repr + kwargs_repr)
        return "{{ %s(%s) }}" % (path, params)

