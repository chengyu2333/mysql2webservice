from pymysql import connect
from pymysql import cursors
from retry import retry
import config
import log
import filter


class DB:
    _db_cursor = None
    _conn = None
    _sql_cursor = 0

    def __init__(self, host, port=27017, user="", password="", db_name=""):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._db_name = db_name
        self.connect(self._db_name)

    def connect(self, db_name):
        self._db_name = db_name if db_name else ""
        if self._db_name:
            self._conn = connect(self._host,
                                 self._user,
                                 self._password,
                                 db_name,
                                 self._port,
                                 cursorclass=cursors.DictCursor)
            self._db_cursor = self._conn.cursor()
        else:
            raise Exception("no database selected")

    def close(self):
        self._db_cursor.close()
        self._conn.close()

    def reset_cursor(self):
        self._sql_cursor = 0

    def get_last_data(self, table, cmp_field, cmp_value, cmp_field_second, cmp_value_second):
        try:
            sql = 'SELECT *,count(*) as count FROM %s WHERE %s >= "%s" GROUP BY %s ORDER BY %s DESC limit 1 ' \
                  % (table, cmp_field, cmp_value, cmp_field, cmp_field)
            self._db_cursor.execute(sql)
            data = self._db_cursor.fetchone()

            if data['count'] > 1:
                # get last data by double field compare
                sql = 'SELECT * FROM %s WHERE %s >= "%s" AND %s>=%s' \
                      % (table, cmp_field, cmp_value, cmp_field_second, cmp_value_second)
                self._db_cursor.execute(sql)
                data = self._db_cursor.fetchone()
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

        if not filter.is_number(cmp_value):cmp_value = "\"" + cmp_value + "\""
        if not filter.is_number(cmp_value_second): cmp_value_ = "\"" + cmp_value_second + "\""


        if cmp_field and cmp_value:
            if cmp_field_second and cmp_value_second:
                # double field compare
                sql = 'SELECT * FROM %s WHERE %s >= %s AND %s>%s ORDER BY %s DESC limit %d,%d'\
                  % (table, cmp_field, cmp_value, cmp_field_second, cmp_value_second, cmp_field, self._sql_cursor, num)
            else:
                sql = 'SELECT * FROM %s WHERE %s > %sORDER BY %s DESC limit %d,%d' \
                      % (table, cmp_field, cmp_value, cmp_field, self._sql_cursor, num)
        else:  # fetch all
            sql = 'SELECT * FROM %s limit %d,%d' % (table, self._sql_cursor, num)

        print("# SQL:", sql)
        self._db_cursor.execute(sql)
        self._sql_cursor += num
        return self._db_cursor.fetchall()

    # get table fields
    def get_table_fields(self, table_name):
        self._db_cursor.execute("select * from " + table_name + " limit 1")
        fields = self._db_cursor.fetchone()
        return [key for key in fields]

    #  create trigger_log detail
    def create_trigger_deatil(self, table_name, event_type, unique_field):
        old_new = "new" if event_type == "insert" else "old"
        strs = []
        strs.append("{")
        for field in unique_field:
            strs.append("\"{f}\":\"\',{o}.{f},\'\",".format(f=field,o=old_new))
        strs.append("}")
        return "".join(strs)

    # create trigger for a table
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
                   detail=self.create_trigger_deatil(table_name, event_type, unique_field)
                   )
        try:
            self._db_cursor.execute(sql)
        except Exception as e:
            log.log_error(str(e))
        # return self._db_cursor.DatabaseError()

    def create_trigger_all(self):
        sql = '''
            DROP TABLE trigger_log;
            CREATE TABLE trigger_log(
            id int PRIMARY KEY auto_increment,
            table_name varchar(64),
            op varchar(8),
            detail text
            );'''
        try:
            self._db_cursor.execute(sql)
        except Exception as e:
            log.log_error(str(e))

        for table in config.tables:
            conf_table = config.tables[table]
            unique_field = None
            event_type = conf_table['trigger']['event_type'] if 'trigger' in conf_table else None
            if not event_type:
                event_type = ("insert", "update", "delete")

            if 'unique_field' in conf_table['trigger'] or not conf_table['trigger']['unique_field']:
                unique_field = conf_table['trigger']['unique_field']
            fields = unique_field if unique_field else self.get_table_fields(table)

            print("field: ", fields)
            for e in event_type:
                log.log_success("create trigger for table: " + table + " event:" + e)
                self.create_trigger(table,e, fields)
        self.close()

    # get trigger log
    @retry(stop_max_attempt_number=config.retry_db,
           stop_max_delay=config.timeout_db * 1000,
           wait_exponential_multiplier=config.slience_db_multiplier * 1000,
           wait_exponential_max=config.slience_db_multiplier_max * 1000)
    def get_trigger_log(self,table_name,operation=(),num=10,pop=True):
        if operation:
            sql = "select * from trigger_log where table_name='%s' and op in %s limit %d" \
                  % (table_name, str(operation), num)
        else:
            sql = "select * from trigger_log where table_name='%s' limit %d" % (table_name, num)
        print("# SQL:", sql)
        self._db_cursor.execute(sql)
        res = self._db_cursor.fetchall()
        if res:
            if pop:
                for row in res:
                    sql = "delete from trigger_log where id=" + str(row['id'])
                    self._db_cursor.execute(sql)
            return res
        else:
            return None

