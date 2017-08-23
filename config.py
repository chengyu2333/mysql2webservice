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
        "cmp_field": "CTIME",  # 用于对比的字段
        "cmp_value_field": "ctime",
        "strict": False,  # 严格模式将仅同步map中的字段,默认使用column名作为post参数名
        "lower": True,  # 将大写转小写,只在非严格模式下有效
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
    #     "post_url": "",
    #     "cmp_field": "CTIME",  # 用于对比的字段
    #     "cmp_value_field": "ctime",
    #     "strict": False,  # 非严格模式将获取全部列,默认使用column名作为post参数名
    #     "map": {
    #     }
    # }
}

