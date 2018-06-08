# -*- coding: utf-8 -*-
import logging
from io import StringIO
import traceback
import os


def exception_to_traceback_string(exp):
    exp_type, exp_value, exp_tb = type(exp), exp, exp.__traceback__

    buffer = StringIO()
    traceback.print_exception(exp_type, exp_value, exp_tb, file=buffer)
    buffer.seek(0)

    return buffer.read()


def render_error(exp):
    try:
        tbstring = exception_to_traceback_string(exp)
    except Exception as renderexp:
        tbstring = "[rendering traceback failed due to %s: %s]" % (renderexp.__class__.__name__, renderexp)
    return "%s: %s\n%s" % (exp.__class__.__name__, exp, tbstring)


def stack_info(top_level_package_name=None):
    traces = traceback.extract_stack()[:-1]
    traces.reverse()

    def shorten_filename(fname):
        if top_level_package_name is None:
            return fname

        parts = fname.split(os.sep)
        ret = []
        for p in reversed(parts):
            ret.insert(0, p)
            if p == top_level_package_name:
                break

        return os.sep.join(ret)

    ret = ""
    for trace in traces:
        filename, lineno, func, text = trace  # @UnusedVariable
        if traces.index(trace) > 0:
            ret += " ==> "
        fname = shorten_filename(filename)
        ret += fname + '@' + func + '#' + str(lineno)

    return ret


def on_task_done(name, ft):
    if ft.cancelled():
        logging.warning("%s canceled" % name)
        return

    error = ft.exception()
    if error:
        logging.error("%s failed - %s" % (name, render_error(error)))
    else:
        logging.debug("%s finished" % name)
