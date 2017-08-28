import requests
import threading
import threadpool
from retrying import retry
import json
import config
import log

COUNT = 0


# batch post_data date to webservice
def post_data(table, data):
    try:
        url = config.tables[table]['post_url']
        global COUNT
        # for d in data:
        #     print(d)

        if config.enable_thread:  # multi thread
            args = []
            for d in data:
                args.append(([url, d], None))
            pool = threadpool.ThreadPool(config.thread_pool_size)
            reqs = threadpool.makeRequests(post_except, args, finished)
            [pool.putRequest(req) for req in reqs]
            pool.wait()
            args.clear()
        else:  # single thread
            for d in data:
                try:
                    res = post_retry(url, d)
                    if res.status_code == 201:
                        COUNT += 1
                    else:
                        log.log_error("post_data data failed\ncode:" + res.status_code + "\nresponse:" + res.text +
                                      "\npost_data data:" + d)
                except Exception as e:
                    log.log_error("server error:" + str(e))
                    continue
    except Exception:
        raise


# TODO 抓取完成时的回调
def finished(*args, **kwargs):
    print("finished")
    print(args[1])
    # print(res)
    # if res.status_code == 201:
    #     COUNT += 1
    # else:
    #     log.log_error(
    #         "post_data data failed\ncode:" + res.status_code + "\nresponse:" + res.text + "\npost_data data:" + d)


# no except handle, for implement retrying
@retry(stop_max_attempt_number=config.retry)
def post_retry(url, d):
    print("post_retry", url, d['genius_uid'])
    return requests.post(url, d, timeout=config.timeout)


# have except handle, for implement multi thread
def post_except(url, d):
    print("post_except:", url, d['genius_uid'])
    try:
        return post_retry(url, d)
    except Exception as e:
        log.log_error("server error:" + str(e))


# from webservice get last data
def get_last(table, cmp_arg, cmp_arg_second=""):
    url = config.tables[table]['get_url']
    # data = requests.get(url, timeout=config.timeout)
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
    data = None
    if data:
        json_data = json.loads(data)
        last_data = json_data[cmp_arg]
        if type(last_data) == str:
            last_data = format(last_data)

        if cmp_arg_second:
            last_data_second = json_data[cmp_arg_second]
            if type(last_data_second) == str:
                last_data_second = format(last_data_second)
            return last_data, last_data_second
        else:
            return last_data
    else:
        return None, None


def format(s):
    s = s.replace('年', '-')
    s = s.replace('月', '-')
    s = s.replace('日', '')
    s = "\"" + s + "\""
    return s