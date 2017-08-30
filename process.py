import database
import req
import config
import log
import filter
db = database.DB()


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
                if not strict:  # non-strict mode
                    if lower:
                        new_key = key.lower()
                    else:
                        new_key = key
            # custom map key
            if process_key:
                new_key = process_key(new_key)

            row_temp[new_key] = item[key]
        data_result.append(row_temp)
        total += 1
    return data_result, total

# synchronize once
def sync():
    total = 0
    # iterate each table
    for table in config.tables:
        log_msg = "start processing table：" + table
        log.log_success(log_msg)

        # get compare flag
        cmp_field = config.tables[table]['cmp_field']
        cmp_arg = config.tables[table]['cmp_arg']
        cmp_field_second = config.tables[table]['cmp_field_second']
        cmp_arg_second = config.tables[table]['cmp_arg_second']

        # get last data from  webservice
        if cmp_field_second:
            last_data, last_data_second = req.get_last(table, cmp_arg, cmp_arg_second)
        else:
            last_data = req.get_last(table, cmp_arg)
            last_data_second = ""

        while True:
            try:  # get newer data from mysql
                data = db.get_next_newer_data(table,
                                              cmp_field=cmp_field,
                                              cmp_value=last_data,
                                              num=config.cache_size,
                                              cmp_field_second=cmp_field_second,
                                              cmp_value_second=last_data_second)
                if not data: break
            except Exception as e:
                log.log_error("get_next_newer_data:" + str(e))
                break

            # mapping filed and argument
            table_map = config.tables[table]['map']
            post_data_list,count = map_dict(data,
                                       table_map, config.
                                       tables[table]['strict'],
                                       config.tables[table]['lower'],
                                       process_value=filter.datetime_to_str,
                                       process_key=None)
            try:
                req.post_data(config.tables[table]['post_url'], post_data_list)
            except Exception as e:
                log.log_error("unknow error:" + str(e))
            finally:
                total += count
                post_data_list.clear()

        log_msg = "processed table:%s finished, total:%d success:%s" % (table, total, req.SUCCESS_COUNT)
        log.log_success(log_msg)
        req.SUCCESS_COUNT = 0
        db.reset_cursor()
