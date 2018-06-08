# -*- coding: utf-8 -*-
from thrift.Thrift import TMessageType


def _generate_VALUES_TO_NAMES(clazz):
    ret = []
    for n, v in clazz.__dict__.items():
        if not n.startswith('_') and n.upper() == n and isinstance(v, int):
            ret.append((n, v))

    ret.sort(key=lambda x: x[1])

    retlist = ['???'] * (ret[-1][1] + 1)

    for n, v in ret:
        retlist[v] = n

    return retlist


def monkey_patch_Thrift_TMessageType():

    name_list = _generate_VALUES_TO_NAMES(TMessageType)

    def get_name(val):
        if val < len(name_list):
            return name_list[val]
        return str(val) + ' (no name)'

    TMessageType.get_name = get_name


monkey_patch_Thrift_TMessageType()
