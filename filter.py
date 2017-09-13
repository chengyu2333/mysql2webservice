import datetime


# test value is numberic
def is_number(obj):
    try:
        float(obj)
        return True
    except:
        return False


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
    return arg
    arg = "\"" + arg + "\""
    return arg
