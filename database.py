from pymysql import connect
from pymysql import cursors
import config


class DB:
    __cursor = None
    __current = 0

    def __init__(self):
        conn = connect(config.db['host'], config.db['user'], config.db['password'], config.db['db_name'], config.db[
            "port"], cursorclass=cursors.DictCursor)
        self.__cursor = conn.cursor()

    def get_test(self):
        self.__cursor.execute("SELECT *,count(CTIME) as count FROM stas_date_info WHERE ctime >= \"2015-02-22 17:40:49\" GROUP BY CTIME ORDER BY CTIME DESC limit 10 ")
        # 如果count > 1，
        # self.__cursor.execute('SELECT * FROM stas_date_info ORDER BY ctime DESC limit 10')
        data = self.__cursor.fetchall()

        # return data

        for column in data[0]:
            print(column,"\t\t",end="")
        print("")
        for row in data:
            # print(row)
            for column in row:
                print(row[column], "\t\t", end="")
            print("")
        print("count:", len(data))

    def get_last_data(self, table, cmp_field, cmp_value, cmp_field_second, cmp_value_second):
        try:
            sql = 'SELECT *,count(*) as count FROM %s WHERE %s >= "%s" GROUP BY %s ORDER BY %s DESC limit 1 ' % (table, cmp_field, cmp_value, cmp_field, cmp_field)
            self.__cursor.execute(sql)
            data = self.__cursor.fetchone()
            if data['count'] > 1:
                # get last data by double field compare
                sql = 'SELECT * FROM %s WHERE %s >= "%s" AND %s>=%s' % (table, cmp_field, cmp_value, cmp_field_second, cmp_value_second)
                self.__cursor.execute(sql)
                data = self.__cursor.fetchone()
            else:
                del data['count']
            return data
        except Exception:
            raise

    def get_next_new_data(self, table, cmp_field="", cmp_value="", cmp_field_second="", cmp_value_second="", num=10):
        try:
            if cmp_field and cmp_value:
                if cmp_field_second and cmp_value_second:
                    # double field compare
                    sql = 'SELECT * FROM %s WHERE %s >= %s AND %s>%s ORDER BY %s DESC limit %d,%d'\
                      % (table, cmp_field, cmp_value, cmp_field_second, cmp_value_second, cmp_field, self.__current, num)
                else:
                    sql = 'SELECT * FROM %s WHERE %s > %sORDER BY %s DESC limit %d,%d' \
                          % (table, cmp_field, cmp_value, cmp_field, self.__current, num)
            else:  # fetch all
                sql = 'SELECT * FROM %s limit %d,%d' % (table, self.__current, num)
            print("# SQL:", sql)
            self.__cursor.execute(sql)
            self.__current += num
            return self.__cursor.fetchall()
        except Exception:
            raise

    def reset_current(self):
        self.__current = 0
