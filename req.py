import requests
import threadpool
from retry import retry
import json
import config
import log
import filter

SUCCESS_COUNT = 0


# batch post data to webservice
def post_data(url, data_list):
    try:
        # for d in data_list:
        #     print(d)

        if config.enable_thread:  # multi thread
            args = []
            for d in data_list:
                args.append(([url, d], None))
            pool = threadpool.ThreadPool(config.thread_pool_size)
            reqs = threadpool.makeRequests(post_except, args, finished)
            [pool.putRequest(req) for req in reqs]
            pool.wait()
            args.clear()
        else:  # single thread
            for d in data_list:
                res = post_except(url, d)
    except Exception:
        raise


# post success callback
def finished(*args):
    global SUCCESS_COUNT
    print("finished  ",args)
    if args[1]:
        print(args[1])
        if args[1].status_code == 201:
            SUCCESS_COUNT += 1
        else:
            log.log_error("post data failed\ncode:%d\nresponse:%s\npost_data data:%s"
                          % (args[1].status_code,args[1].text,args[0]))
    else:
        pass


# no exception handle, for implement retrying
@retry(stop_max_attempt_number=config.retry_http,
       wait_exponential_multiplier=config.slience_http_multiplier*1000,
       wait_exponential_max=config.slience_http_multiplier_max*1000)
def post_retry(url, data, is_json=True):
    print("try…… ", end="")
    try:
        if is_json:
            return requests.post(url, json=json.dumps(data), timeout=config.timeout_http)
        else:
            return requests.post(url, data=data, timeout=config.timeout_http)
    except Exception as e:
        print(str(e))
        raise

# have exception handle, for implement multi thread
def post_except(url, data, is_json=True):
    global SUCCESS_COUNT
    print("post_except:", url, " ", )
    try:
        res = post_retry(url, data, is_json)
        if res.status_code == 201:
            SUCCESS_COUNT += 1
        else:
            log.log_error("post data failed\ncode:%d\nresponse:%s\npost_data data:%s"
                          % (res.status_code, "", data))
        return res
    except Exception as e:
        log.log_error("server error:" + str(e) + "\ndata:" + str(d))


# get last flag from webservice
def get_last(table, cmp_arg, cmp_arg_second=""):
    url = config.tables[table]['get_url']
    # data = requests.get(url, timeout_http=config.timeout_http)
    data = '''
    {
        "id": "599ba72fa54d752382002d82",
        "range": "hahaha",
        "seq": 134587,
        "ctime": "2017年02月22日 17:40:49",
        "mtime": "2017年08月04日 19:05:28",
        "listdate": "2017年08月07日 00:00:00",
        "end_list_date": null,
        "comcode": 210624169,
        "status_type": 0,
        "inner_code": null,
        "pre_mongified_id": 101029299,
        "stk_type": 1,
        "isvalid": "1.00",
        "genius_uid": 12713324837,
        "esname": "FRONTIER",
        "trade_mkt": "股份转让系统",
        "stockcode": "871595",
        "stocksname": "耐戈友",
        "chi_spel": null,
        "tra_curncy": null,
        "book_val": null,
        "fac_curncy": null,
        "list_sec": null,
        "ia_name": null
    }
    '''
    # data = None
    if data:
        json_data = json.loads(data)
        last_data = json_data[cmp_arg]
        if type(last_data) == str:
            last_data = filter.convert_arg_datetime(last_data)

        if cmp_arg_second:
            last_data_second = json_data[cmp_arg_second]
            if type(last_data_second) == str:
                last_data_second = filter.convert_arg_datetime(last_data_second)
            return last_data, last_data_second
        else:
            return last_data
    else:
        return None, None


def get_option(table):
    url = config.tables[table]['get_url']
    data = requests.options(url).text
    data = json.loads(data)['actions']['POST']
    # return data
    for i in data:
        print(data[i])

# get_option("stas_date_info")
