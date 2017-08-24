# mysql2webservice
一个MySQL同步到webService的工具
## 配置文件
```
db = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "db_name": "test",
    "port": 3366
}

max_thread = 10

tables = {
    "stas_date_info": {
        "post_url": "http://192.168.1.92:8004/api/date/",
        "get_url": "http://192.168.1.92:8004/api/date/",
        "sync_all": False,  # 是否同步全部数据（自动设置）
        "cmp_field": "CTIME",  # 用于对比的字段名(数据库)
        "cmp_arg": "ctime",  # 用于对比的参数名(get的数据)
        "cmp_field_second": "SEQ",  # 如果第一个对比值不唯一，则启用第二对比条件,为空则不启用
        "cmp_arg_second": "seq",
        "strict": False,  # 严格模式将仅同步map中的字段,默认使用column名作为post参数名
        "lower": True,  # 将column name转小写,只在非严格模式下有效
        "map": {
            # column:post argument
            # "CTIME": "ctime",
            # "SEQ": "seq",
            # "MTIME": "mtime",
            # "STARTDATE": "startdate",
            # "ENDDATE": "enddate",
            # "ISS_ID": "iss_id",
            # "DATE_TYPE_CODE":"date_type_code",
            # "COMCODE":"comcode",
            # "IF_INTERVAL":"if_interval",
            # "ISVALID": "isvalid",
            # "GENIUS_UID": "genius_uid"
        }
    },
    # "stas_achieve_report": {
    # 第二个表的配置
    # }
}
```
