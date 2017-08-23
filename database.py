from pymysql import connect
from pymysql import cursors
import config
import datetime


class DB:
    __cursor = None
    __current = 0

    def __init__(self):
        conn = connect("localhost", "root", "root", "test", 3366, cursorclass=cursors.DictCursor)
        self.__cursor = conn.cursor()

    def get_text(self):
        self.__cursor.execute('SELECT * FROM stas_date_info WHERE ctime > "2017-02-22 09:40:18" ORDER BY ctime DESC limit 10')
        # self.__cursor.execute('SELECT * FROM stas_date_info ORDER BY ctime DESC limit 10')
        data = self.__cursor.fetchall()
        for column in data[0]:
            print(column,"\t\t",end="")
        print("")
        for row in data:
            # print(row)
            for column in row:
                print(row[column], "\t\t", end="")
            print("")
        print("count:", len(data))

    # 获取新数据
    def get_new_data(self, table, cmp_field, cmp_value, num):
        sql = 'SELECT * FROM %s WHERE %s > "%s" ORDER BY ctime DESC limit %d' % (table, cmp_field, cmp_value, num)
        print("# SQL:",sql)
        self.__cursor.execute(sql)
        return self.__cursor.fetchall()

    # 获取下一批新数据
    def get_next_new_data(self, table, cmp_field, cmp_value, num):
        sql = 'SELECT * FROM %s WHERE %s > "%s" ORDER BY ctime DESC limit %d,%d' % (table, cmp_field, cmp_value, self.__current, num)
        print("# SQL:",sql)
        self.__cursor.execute(sql)
        self.__current += num
        return self.__cursor.fetchall()

    def reset_current(self):
        self.__current = 0


# data = DB().get_new_data("stas_date_info", "CTIME", "2017-02-22 09:40:18", 10)
# print(data)
# DB().get_last()