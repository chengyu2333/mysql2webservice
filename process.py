import database
import datetime
import req
import config
import log
import filter
db = database.DB()


def sync():
    total = 0
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


        post_data = []
        while True:
            try:  # get newer data from mysql
                data = db.get_next_newer_data(table, cmp_field=cmp_field, cmp_value=last_data, num=config.cache_size, cmp_field_second=cmp_field_second, cmp_value_second=last_data_second)
                if not data: break
            except Exception as e:
                log.log_error("get_next_newer_data:" + str(e))
                break

            # mapping filed and argument
            table_map = config.tables[table]['map']
            for row in data:  # iteration data from db
                row_temp = {}
                for column in row:  # iteration filed from db
                    # convert each field
                    row[column] = filter.datetime_to_str(row[column])

                    # if have mapping relation
                    if column in table_map:
                        col_name = table_map[column]
                        row_temp[col_name] = row[column]
                    else:  # if haven't mapping relation
                        if not config.tables[table]['strict']:  # non-strict mode
                            if config.tables[table]['lower']:
                                row_temp[column.lower()] = row[column]
                            else:
                                row_temp[column] = row[column]
                print("·", end="")
                post_data.append(row_temp)
                total += 1

            try:
                req.post_data(table, post_data)
            except Exception as e:
                log.log_error("unknow error:" + str(e))
            finally:
                post_data.clear()

        db.reset_cursor()
        log_msg = "processed table:%s finished, total:%d success:%s" % (table, total, req.COUNT)
        log.log_success(log_msg)
        req.COUNT = 0

