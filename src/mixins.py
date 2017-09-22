import luigi
import hashlib
import warnings

class ThreadsMixin(object):
    """Sets the thread usage based on the `threads` property.
    
    Overrides `resources`.
    """
    @property
    def resources(self):
        return {"threads": self.threads}

class HashOutputMixin(object):
    """Determine output path based on hash of parameters (ordered by parameter name).
    
    Set `LOG_PATH` to something with a `{}` to set the log location. Example:
        `LOG_PATH = "logs/{}.log"`

    Overrides `output()`.

    An additional parameter `omit` is provided and used by the
    `rename_logs.py` script to adjust log names after adding new
    parameters.
    """
    def output(self, omit=set()):
        hash = hashlib.sha256()
        for param, value in sorted(self.to_str_params(only_significant=True).items(), key=lambda t: t[0]):
            if param not in omit:
                hash.update(param.encode('utf-8'))
                hash.update(value.encode('utf-8'))

        return luigi.LocalTarget(self.LOG_PATH.format(hash.hexdigest()))

    def encode(self, locale):
        """Janky means to allow tasks of this kind as parameters to other HashOutputMixin Tasks."""
        return self.output().path.encode(locale)

class DictOutputMixin(object):
    """Determine output path as a sequence of `_`-separated `key=value` pairs (ordered by parameter name).
    
    Set `LOG_PATH` to something with a `{}` to set the log location. Example:
        `LOG_PATH = "logs/{}.log"`

    Overrides `output()`, `encode(locale)`, and defines a new function `sanitize(str)`.

    An additional parameter `omit` is provided and used by the
    `rename_logs.py` script to adjust log names after adding new
    parameters.
    """
    def sanitize(s):
        return s.replace("/", "-").replace("_","-")
    def output(self, omit=set()):
        pairs = []
        for param, value in sorted(self.to_str_params(only_significant=True).items(), key=lambda t: t[0]):
            if param not in omit:
                pairs += ["{}={}".format(DictOutputMixin.sanitize(param), DictOutputMixin.sanitize(value))]

        return luigi.LocalTarget(self.LOG_PATH.format("_".join(pairs)))

    def encode(self, locale):
        """Janky means to allow tasks of this kind as parameters to other HashOutputMixin Tasks."""
        return self.output().path.encode(locale)

class TempPathMixin(object):
    """Wraps `run()`, setting `self._tmp_output` to the path of a
    temporary file which will be moved to the location given by
    `output()` after the run completes.
    
    Only usable on tasks with a single output."""
    def run(self):
        self.output().makedirs()
        with self.output().temporary_path() as path:
            self._tmp_output = path
            super().run() 

class RustBacktraceMixin(object):
    """Turns on Rust backtraces. Only works for `ExternalProgramTask`
    instances."""
    def program_env(self):
        return {
            "RUST_BACKTRACE": 1
        }

class TaskParameter(luigi.Parameter):
    """A parameter which is required to be the specific `kind` of Task.
    
    Technically works for any class, but the *intent* is to use this as
    a means of passing tasks as parameters."""
    def __init__(self, *args, kind=luigi.Task, **kwargs):
        super().__init__(*args, **kwargs)
        self.kind = kind
    def _warn_on_wrong_param_type(self, param_name, param_value):
        if self.__class__ != TaskParameter:
            return
        if not isinstance(param_value, self.kind):
            warnings.warn('Parameter "{}" with value "{}" is not an instance of {}'.format(param_name, param_value, self.kind))
