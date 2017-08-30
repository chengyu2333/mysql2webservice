from pymysql import connect
from pymysql import cursors
from retry import retry
import config


class DB:
    __db_cursor = None
    __sql_cursor = 0

    def __init__(self):
        conn = connect(config.db['host'],
                       config.db['user'],
                       config.db['password'],
                       config.db['db_name'],
                       config.db["port"],
                       cursorclass=cursors.DictCursor)
        self.__db_cursor = conn.cursor()

    def get_last_data(self, table, cmp_field, cmp_value, cmp_field_second, cmp_value_second):
        try:
            sql = 'SELECT *,count(*) as count FROM %s WHERE %s >= "%s" GROUP BY %s ORDER BY %s DESC limit 1 ' \
                  % (table, cmp_field, cmp_value, cmp_field, cmp_field)
            self.__db_cursor.execute(sql)
            data = self.__db_cursor.fetchone()

            if data['count'] > 1:
                # get last data by double field compare
                sql = 'SELECT * FROM %s WHERE %s >= "%s" AND %s>=%s' \
                      % (table, cmp_field, cmp_value, cmp_field_second, cmp_value_second)
                self.__db_cursor.execute(sql)
                data = self.__db_cursor.fetchone()
            else:
                del data['count']

            return data
        except Exception:
            raise

    @retry(stop_max_attempt_number = config.retry_db,
           stop_max_delay=config.timeout_db*1000,
           wait_exponential_multiplier=config.slience_db_multiplier*1000,
           wait_exponential_max=config.slience_db_multiplier_max * 1000)
    def get_next_newer_data(self, table, cmp_field="", cmp_value="", cmp_field_second="", cmp_value_second="", num=10):
        if cmp_field and cmp_value:
            if cmp_field_second and cmp_value_second:
                # double field compare
                sql = 'SELECT * FROM %s WHERE %s >= %s AND %s>%s ORDER BY %s DESC limit %d,%d'\
                  % (table, cmp_field, cmp_value, cmp_field_second, cmp_value_second, cmp_field, self.__sql_cursor, num)
            else:
                sql = 'SELECT * FROM %s WHERE %s > %sORDER BY %s DESC limit %d,%d' \
                      % (table, cmp_field, cmp_value, cmp_field, self.__sql_cursor, num)
        else:  # fetch all
            sql = 'SELECT * FROM %s limit %d,%d' % (table, self.__sql_cursor, num)

        print("# SQL:", sql)
        self.__db_cursor.execute(sql)
        self.__sql_cursor += num
        return self.__db_cursor.fetchall()

    def reset_cursor(self):
        self.__sql_cursor = 0
