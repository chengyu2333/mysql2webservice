from database import DB
from req import GetReq
import config

db = DB(host=config.db['host'],
        port=config.db['port'],
        user=config.db['user'],
        password=config.db['password'],
        db_name=config.db['db_name'])


def check_config_map(table_name):
    strict = config.tables[table_name]['strict']
    lower = config.tables[table_name]['lower']
    table_map = config.tables[table_name]['map']
    get_url = config.tables[table_name]['get_url']
    fields_conf = []
    for m in table_map:
        if isinstance(m,str):
            fields_conf.append(m)
        else:
            v = table_map[m]
            if "key" in v:
                fields_conf.append(v['key'])
            else:
                fields_conf.append(m)

    fields_db = db.get_table_fields(table_name)
    options = GetReq.get_options(get_url)
    if lower:
        fields_db = map(lambda s:s.lower(), fields_db)
        fields_conf = map(lambda  s:s.lower(), fields_conf)

    for o in options:
        # if field is required
        if options[o]['required']:
            # if field not in config
            if o not in fields_conf:
                if strict:
                    raise Exception(o," is required ")
                else:
                    if o not in fields_db:
                        raise Exception(o, " is required ")
    print(table_name," : map config check OK")

        # options[o]['read_only']
        # options[o]['type']


def check_config_map_all():
    for table in config.tables:
        check_config_map(table)
