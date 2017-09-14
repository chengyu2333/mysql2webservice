# map dict list
def map_dict(data_source, map_rule, strict=False, lower=True):
    '''
    # for example:
    # data_source:
        [{'ID': 123, 'USER': 'chengyu'},{'ID': 001, 'USER': 'user'}]
    # map_rule:
        {'ID':'uid','USER':'username'}
    # result:
        [{'uid': 123, 'username': 'chengyu'},{'ID': 001, 'username': 'user'}]
    '''
    total = 0
    data_result = []
    for data_item in data_source:  # iteration data
        row_temp = {}
        for old_key in data_item:  # iteration filed
            new_key = None
            new_value = data_item[old_key]
            if lower: new_key = old_key.lower()

            if old_key in map_rule:
                # if map config is string
                if str(type(map_rule[old_key]))=="<class 'str'>":
                    map_rule[old_key] = {"key":map_rule[old_key]}

                # need map
                if "key" in map_rule[old_key]:
                    new_key = map_rule[old_key]['key']
                else:
                    if not strict: new_key = old_key
                if "apply" in map_rule[old_key]:
                    op = map_rule[old_key]['apply']
                    new_value = map_apply(data_item[old_key], op)
            else:  # if haven't mapping relation
                if not strict: new_key = old_key

            if new_key: # combine
                if lower: new_key = new_key.lower()
                if map_rule:
                    row_temp[new_key] = new_value

            print( "|" ,type(row_temp[new_key]), "|" ,new_key, "|",row_temp[new_key], "|")
        data_result.append(row_temp)
        total += 1
    return data_result, total

def map_apply(obj, op):
    if str(type(op)) == "<class 'function'>":
        return op(obj)
    if op==str or op==int or op==float:
        return op(obj)
    if type(op) == str:
        if op=="dt_rfc3339":
            return str(obj).replace(" ", "T")
        if op=="none2blank":
            if str(type(obj))=="<class 'NoneType'>":
                return ""
        if op=="dt_ch2":
            pass
    raise Exception("没有这种操作")

