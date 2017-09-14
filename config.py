db = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "db_name": "test",
    "port": 3366
}

cycle_time = 10  # 运行周期
enable_thread = False  # 启用线程
thread_pool_size = 1  # 线程池大小
cache_size = 1  # 缓存大小

# 超时时间
timeout_http = 1
timeout_db = 1

# 重试等待时间指数增长
slience_db_multiplier = 2
slience_db_multiplier_max = 6
slience_http_multiplier = 2
slience_http_multiplier_max = 6

# 超时/出错重试次数
retry_http = 5
retry_db = 5

print_log = True

# POST
post_as_json = False
post_success_code = 201

tables = {
    "stas_date_info": {
        "post_url": "http://127.0.0.1",
        "get_url": "http://api.chinaipo.com/zh-hans/api/article/?format=json",
        # 对比标志，必须是递增字段
        "cmp_field": "CTIME",  # 用于对比的字段名(数据库)
        "cmp_arg": "ctime",  # 用于对比的参数名(get的数据)
        "cmp_field_second": "SEQ",  # 第二判定条件，为空则不启用
        "cmp_arg_second": "seq",
        # 通过mysql触发器精准同步数据
        "trigger": {
            "event_type": ("insert","update","delete"),
            # trigger要记录的字段，为空则记录全部字段
            "unique_field": [
                # "CTIME"
            ],
        },
        "strict": False,  # 严格模式将仅同步配置的映射关系中的字段
        "lower": True,  # 将column转小写
        # 数据库字段和POST参数映射关系,默认使用field作为post参数
        "map": {
            # 格式 db_key:{post_key,post_type,value_filter}
            "SEQ": "seq1",
            "CTIME": {"apply":"dt_rfc3339"},
            "MTIME": {"apply":lambda x:str(x)},
            "ISVALID": {"key":"is_valid","apply":int},

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
    # }
}


