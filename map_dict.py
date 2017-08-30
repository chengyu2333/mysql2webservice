# map dict list
def map_dict(data_source, map_rule, strict=False, lower=True, process_key=None, process_value=None):
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
    for item in data_source:  # iteration data
        row_temp = {}
        for key in item:  # iteration filed
            new_key = None
            new_value = None

            # costem map value
            if process_value:
                item[key] = process_value(item[key])

            # map key
            if key in map_rule:
                new_key = map_rule[key]
            else:  # if haven't mapping relation
                if strict:  # non-strict mode
                    new_key = None
                else:
                    if lower:
                        new_key = key.lower()
                    else:
                        new_key = key
            # custom map key
            if process_key:
                new_key = process_key(new_key)
            if new_key:
                row_temp[new_key] = item[key]
        data_result.append(row_temp)
        total += 1
    return data_result, total