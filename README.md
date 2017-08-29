# mysql2webservice
一个MySQL同步到webService的工具
## Feature
### 多线程，出错重试
### 支持增量更新，增量更新需要满足以下条件：
- 数据库中有递增字段
- webservice提供获取最新数据的接口
## Tip
- 默认一直检测运行，可结合crontab或其他任务调度工具
- 一般情况下只需要修改一下config.py文件和filter文件

## config.py示例
```
# 数据库配置
db = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "db_name": "test",
    "port": 3366
}

enable_thread = True  # 启用线程
thread_pool_size = 10  # 线程池大小
cache_size = 10  # 缓存大小
timeout = 1  # post超时时间
# 出错重试次数
retry_post = 3
retry_mysql = 3
print_log = True  # 输出日志到控制台

tables = {
    "stas_date_info1": {
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
    "stas_achieve_report": {
        "post_url": "http://192.168.1.92:8004/api/date/",
        "get_url": "",
        "cmp_field": "CTIME",
        "cmp_arg": "ctime",
        "cmp_field_second": "",
        "cmp_arg_second": "",
        "strict": False,
        "lower": True,
        "map": {
            # 如果未启用严格模式，且数据库和post字段完全对应，那么这里不用配置
        }
    }
}


```
