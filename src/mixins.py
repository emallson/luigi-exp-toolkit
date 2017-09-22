import luigi
import hashlib
import warnings

class ThreadsMixin(object):
    @property
    def resources(self):
        return {"threads": self.threads}

class HashOutputMixin(object):
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
    def run(self):
        self.output().makedirs()
        with self.output().temporary_path() as path:
            self._tmp_output = path
            super().run() 

class RustBacktraceMixin(object):
    def program_env(self):
        return {
            "RUST_BACKTRACE": 1
        }

class TaskParameter(luigi.Parameter):
    def __init__(self, *args, kind=luigi.Task, **kwargs):
        super().__init__(*args, **kwargs)
        self.kind = kind
    def _warn_on_wrong_param_type(self, param_name, param_value):
        if self.__class__ != TaskParameter:
            return
        if not isinstance(param_value, self.kind):
            warnings.warn('Parameter "{}" with value "{}" is not an instance of {}'.format(param_name, param_value, self.kind))
