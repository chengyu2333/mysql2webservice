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

    def reset_cursor(self):
        self.__sql_cursor = 0

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

    def get_fields(self,table_name):
        self.__db_cursor.execute("select * from "+table_name+" limit 1")
        fields = self.__db_cursor.fetchone()
        # return ['SEQ','CTIME']
        return [key for key in fields]

    def create_deatil(self,table_name,event_type,unique_field):
        old_new = "new" if event_type == "insert" else "old"
        strs = []
        strs.append("{")
        for field in unique_field:
            strs.append("\"{f}\":\"\',{o}.{f},\'\",".format(f=field,o=old_new))
        strs.append("}")
        return "".join(strs)

    def create_trigger(self,table_name, event_type, unique_field):

        sql = '''
            DROP TRIGGER if EXISTS t_{event_type}_{table_name};
            CREATE TRIGGER t_{event_type}_{table_name}
            AFTER {event_type} ON {table_name}
            FOR EACH ROW
            BEGIN
                 insert into trigger_log(table_name,op,detail) values("{table_name}","{event_type}",CONCAT_WS('','{detail}'));
            END;
        '''
        sql = sql.format(event_type=event_type,
                   table_name=table_name,
                   detail=self.create_deatil(table_name,event_type, unique_field)
                   )
        # print(sql)
        re = self.__db_cursor.execute(sql)
        return self.__db_cursor.DatabaseError()

    def create_trigger_all(self):
        for table in config.tables:
            print("create trigger for table: " + table)
            unique_field = None
            if 'unique_field' in config.tables[table] or not config.tables[table]:
                unique_field = config.tables[table]['unique_field']
            fields = unique_field if unique_field else self.get_fields(table)
            print("field: ", fields)
            re = self.create_trigger(table,"update", fields)
            print(str(re))

    # get trigger log
    def get_trigger_log(self,table_name,operation=(),num=10,pop=True):
        if operation:
            sql = "select * from trigger_log where table_name='%s' and op in %s limit %d" \
                  % (table_name, str(operation), num)
        else:
            sql = "select * from trigger_log where table_name='%s' limit %d" % (table_name, num)
        print(sql)
        self.__db_cursor.execute(sql)
        res = self.__db_cursor.fetchall()
        if res:
            if pop:
                for row in res:
                    sql = "delete from trigger_log where id=" + str(row['id'])
                    self.__db_cursor.execute(sql)
            return res
        else:
            return None


# DB().create_trigger_all()
# re = DB().get_trigger_log("stas_date_info",("update","insert"),10,False)
# print(re)