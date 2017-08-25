import database
import req
import config
import log
db = database.DB()


def process():
    total = 0
    for table in config.tables:
        log_msg = "start processing table：" + table
        log.log_success(log_msg)

        sync_all = config.tables[table]['sync_all']
        if sync_all:
            cmp_field = 0
            cmp_field_second = 0
            last_data = 0
            last_data_second = 0
        else:
            cmp_field = config.tables[table]['cmp_field']
            cmp_arg = config.tables[table]['cmp_arg']
            cmp_field_second = config.tables[table]['cmp_field_second']
            cmp_arg_second = config.tables[table]['cmp_arg_second']

            # get_last webservice last data
            if cmp_field_second:
                last_data, last_data_second = req.get_last(table, cmp_arg, cmp_arg_second)
            else:
                last_data = req.get_last(table, cmp_arg)
                last_data_second = ""

        post_data = []

        # get last new data from mysql
        while True:
            try:
                data = db.get_next_new_data(table, cmp_field=cmp_field, cmp_value=last_data, num=config.max_thread, cmp_field_second=cmp_field_second, cmp_value_second=last_data_second)
            except Exception as e:
                log.log_error("get_next_new_data:" + str(e))
                continue

            if not data:
                break

            # mapping filed and argument
            table_map = config.tables[table]['map']
            for row in data:
                row_temp = {}
                for column in row:
                    # print(column)
                    if column in table_map:
                        # if have mapping relation
                        col_name = map[column]
                        row_temp[col_name] = row[column]
                    else:
                        # if haven't mapping relation
                        if not config.tables[table]['strict']:
                            if config.tables[table]['lower']:
                                row_temp[column.lower()] = row[column]
                            else:
                                row_temp[column] = row[column]
                print("·", end="")

                post_data.append(row_temp)
                total += 1
            try:
                # post_data the post_data
                req.post_data(table, post_data)
            except Exception as e:
                log.log_error("unknow error:" + str(e))

        db.reset_current()
        log_msg = "processed table:%s finished, total:%d success:%s" % (table, total, req.COUNT)
        log.log_success(log_msg)
        req.COUNT = 0

process()