import json
import database
import req
import config
import log
import filter
from map_dict import map_dict
db = database.DB()


# synchronize once
def sync_api():
    total = 0
    # iterate each table
    for table in config.tables:
        log_msg = "start processing table：" + table
        log.log_success(log_msg)
        conf_table = config.tables[table]

        # get compare flag
        cmp_field = conf_table['cmp_field'] if 'cmp_field' in conf_table else None
        cmp_arg = conf_table['cmp_arg'] if 'cmp_arg' in conf_table else None
        cmp_field_second = conf_table['cmp_field_second'] if 'cmp_field_second' in conf_table else None
        cmp_arg_second = conf_table['cmp_arg_second'] if 'cmp_arg_second' in conf_table else None

        # get last data from  webservice
        if cmp_field_second:
            last_data, last_data_second = req.get_last(table, cmp_arg, cmp_arg_second)
        elif cmp_field:
            last_data = req.get_last(table, cmp_arg)
            last_data_second = None
        else:
            last_data_second, last_data = None, None

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
            table_map = conf_table['map']
            post_data_list,count = map_dict(data,
                                            table_map,
                                            conf_table['strict'],
                                            conf_table['lower'])
            try:
                req.post_data(conf_table['post_url'], post_data_list)
            except Exception as e:
                log.log_error("unknow error:" + str(e))
            finally:
                total += count
                post_data_list.clear()

        log_msg = "processed table:%s finished, total:%d success:%s" % (table, total, req.SUCCESS_COUNT)
        log.log_success(log_msg)
        req.SUCCESS_COUNT = 0
        db.reset_cursor()


def sync_trigger():
    total = 0
    # iterate each table
    for table in config.tables:
        log_msg = "start processing table：" + table
        log.log_success(log_msg)
        conf_table = config.tables[table]
        sync_all = conf_table['sync_all'] if 'sync_all' in conf_table else False

        while True:
            try:  # get newer data from mysql
                data = db.get_trigger_log(table,("insert","update","delete"),config.cache_size)
                if not data: break
            except Exception as e:
                log.log_error("get_trigger_log:" + str(e))
                break
            post_data_list = []
            for row in data:
                print(row)
                new_row = row['detail'][:-2] + "}"
                new_row = json.loads(new_row)
                post_data_list.append(new_row)

            # mapping filed and argument
            table_map = conf_table['map']
            post_data_list, count = map_dict(post_data_list,
                                             table_map,
                                             conf_table['strict'],
                                             conf_table['lower'],
                                             process_post_keys=filter.time_rfc3339,
                                             process_db_keys=None)
            print(post_data_list)
            try:
                req.post_data(conf_table['post_url'], post_data_list)
            except Exception as e:
                log.log_error("unknow error:" + str(e))
            finally:
                total += count
                post_data_list.clear()

        log_msg = "processed table:%s finished, total:%d success:%s" % (table, total, req.SUCCESS_COUNT)
        log.log_success(log_msg)
        req.SUCCESS_COUNT = 0
        db.reset_cursor()