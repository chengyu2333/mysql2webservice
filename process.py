import json
from database import DB
from req import Req
import config
import log
import filter
import util
from map_dict import map_dict
db = DB(host=config.db['host'],
        port=config.db['port'],
        user=config.db['user'],
        password=config.db['password'],
        db_name=config.db['db_name'])
req = Req(retry_http=config.retry_http,
          slience_http_multiplier=config.slience_db_multiplier,
          slience_http_multiplier_max=config.slience_http_multiplier_max,
          timeout_http=config.timeout_http)


class Sync:
    def __init__(self):
        pass
    def sync_table(self):
        pass

# synchronize once
def sync_by_api():
    total = 0
    for table in config.tables:
        log_msg = "start processing table：" + table
        log.log_success(log_msg)

        conf_table = config.tables[table]
        get_url = conf_table['get_url']

        # get compare flag
        cmp_field = conf_table['cmp_field'] if 'cmp_field' in conf_table else None
        cmp_arg = conf_table['cmp_arg'] if 'cmp_arg' in conf_table else None
        cmp_field_second = conf_table['cmp_field_second'] if 'cmp_field_second' in conf_table else None
        cmp_arg_second = conf_table['cmp_arg_second'] if 'cmp_arg_second' in conf_table else None

        # get last data from  webservice
        if cmp_field_second:
            last_data, last_data_second = req.get_last_flag(get_url, cmp_arg, cmp_arg_second)
        elif cmp_field:
            last_data = req.get_last_flag(get_url, cmp_arg)
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

        log_msg = "processed table:%s __callback, total:%d success:%s" % (table, total, req.success_count)
        log.log_success(log_msg)
        req.success_count = 0
        db.reset_cursor()


def sync_by_trigger():
    total = 0
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

        log_msg = "processed table:%s __callback, total:%d success:%s" % (table, total, req.success_count)
        log.log_success(log_msg)
        req.success_count = 0
        db.reset_cursor()