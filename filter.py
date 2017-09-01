import datetime


def time_rfc3339(field):
    if type(field) == datetime.datetime:
        return str(field).replace(" ", "T")
    else:
        return field


def str_obj(string):
    if string=="":
        return None


def convert_arg_datetime(arg):
    arg = arg.replace('年', '-')
    arg = arg.replace('月', '-')
    arg = arg.replace('日', '')
    arg = "\"" + arg + "\""
    return arg
