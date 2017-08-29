import datetime


def datetime_to_str(field):
    if type(field) == datetime.datetime:
        return str(field).replace(" ", "T")

def convert_arg_datetime(arg):
    arg = arg.replace('年', '-')
    arg = arg.replace('月', '-')
    arg = arg.replace('日', '')
    arg = "\"" + arg + "\""
    return arg