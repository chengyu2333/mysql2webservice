import database
import req
import config
import log
import filter
from map_dict import map_dict
db = database.DB()


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
