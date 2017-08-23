import database
import req
import config
db = database.DB()


def process():
    count = 0

    for table in config.tables:
        print("\n※Start process table ：", table)
        cmp_field = config.tables[table]['cmp_field']
        cmp_value_field = config.tables[table]['cmp_value_field']
        post_data = []

        # get_last webservice last data
        last_data = req.get_last(table)
        cmp_value = last_data[cmp_value_field]
        cmp_value = cmp_value.replace('年', '-')
        cmp_value = cmp_value.replace('月', '-')
        cmp_value = cmp_value.replace('日', '')

        # get_last new data from mysql
        while True:
            data = db.get_next_new_data(table, cmp_field, cmp_value, config.max_thread)
            if not data:
                break

            # map
            table_map = config.tables[table]['map']
            for row in data:
                row_temp = {}
                for column in row:
                    # print(column)
                    if column in table_map:
                        # 如果有映射关系
                        col_name = map[column]
                        row_temp[col_name] = row[column]
                    else:
                        # 没有映射关系
                        if not config.tables[table]['strict']:
                            if config.tables[table]['lower']:
                                row_temp[column.lower()] = row[column]
                            else:
                                row_temp[column] = row[column]
                print("·", end="")

                post_data.append(row_temp)
                count += 1
            # post the post_data
            req.post(table, post_data)

        db.reset_current()
        print("√ processed table:",table, "  count:",count)

process()