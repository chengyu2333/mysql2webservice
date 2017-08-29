db = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "db_name": "test",
    "port": 3366
}

enable_thread = False  # 启用线程
thread_pool_size = 1  # 线程池大小
cache_size = 1  # 缓存大小
# 超时时间
timeout_http = 1
timeout_db = 1
# 重试等待时间（弃用）
slience_db = 5
slience_http = 5
# 重试等待时间指数增长时底数
slience_db_multiplier = 2
slience_http_multiplier = 2
# 超时/出错重试次数
retry_post = 5
retry_mysql = 5
print_log = True  # 输出日志到控制台

tables = {
    "stas_date_info": {
        "post_url": "http://192.168.1.92:8004/api/date/",
        "get_url": "",
        # 对比标志，必须是递增字段
        "cmp_field": "CTIME",  # 用于对比的字段名(数据库)
        "cmp_arg": "ctime",  # 用于对比的参数名(get的数据)
        "cmp_field_second": "SEQ",  # 第二判定条件，为空则不启用
        "cmp_arg_second": "seq",

        "strict": False,  # 严格模式将仅同步配置的map中的字段
        "lower": True,  # 将column转小写,只在非严格模式下有效
        # 数据库字段和POST参数映射关系,默认使用column作为post参数
        "map": {
            # 格式 column:post argument
            "CTIME": "ctime",
            "SEQ": "seq",
            "MTIME": "mtime",
            "STARTDATE": "startdate",
            "ENDDATE": "enddate",
            "ISS_ID": "iss_id",
            "DATE_TYPE_CODE":"date_type_code",
            "COMCODE":"comcode",
            "IF_INTERVAL":"if_interval",
            "ISVALID": "isvalid",
            "GENIUS_UID": "genius_uid"
        }
    },
    # "stas_achieve_report": {
    #     "post_url": "http://192.168.1.92:8004/api/date/",
    #     "get_url": "",
    #     "cmp_field": "CTIME",
    #     "cmp_arg": "ctime",
    #     "cmp_field_second": "",
    #     "cmp_arg_second": "",
    #     "strict": False,
    #     "lower": True,
    #     "map": {
    #         # 如果未启用严格模式，且数据库和post字段完全对应，那么这里不用配置
    #     }
    # }
}

