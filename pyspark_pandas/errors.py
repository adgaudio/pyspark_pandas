from . import log


def log_raise_if(cond, msg, extra=None, err_kls=None):
    if cond:
        log_raise(msg, extra, err_kls)


def log_raise(msg, extra=None, err_kls=None):
    log.error(msg, extra=extra or {})
    raise (err_kls or Exception)(
        "%s %s" % (msg, "=".join(extra.items())))


class ExceededMaxIterations(Exception):
    pass


class TooFewRows(Exception):
    pass


class SamplingError(Exception):
    pass
