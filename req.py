import requests
import socket
import json
import config
import log


COUNT = 0
requests.ConnectTimeout = config.timeout


# batch post_data date to webservice
def post_data(table, data):
    try:
        # for d in data:
            # print(d)
        global COUNT
        url = config.tables[table]['post_url']

        for d in data:
            try:
                res = requests.post(url, d)
            except Exception as e:
                log.log_error("server error:" + str(e))
                continue
            if res.status_code == 201:
                COUNT += 1
            else:
                log.log_error("post_data data failed\ncode:" + res.status_code + "\nresponse:" + res.text + "\npost_data data:" + d)
    except Exception:
        raise

# from webservice get last data
def get_last(table, cmp_arg, cmp_arg_second=""):
    url = config.tables[table]['get_url']
    # data = requests.get(url)
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
    json_data = json.loads(data)
    # TODO 如果请求为空则修改配置同步全部
    last_data = json_data[cmp_arg]
    if type(last_data) == str:
        last_data = last_data.replace('年', '-')
        last_data = last_data.replace('月', '-')
        last_data = last_data.replace('日', '')
        last_data = "\"" + last_data + "\""

    if cmp_arg_second:
        last_data_second = json_data[cmp_arg_second]
        if type(last_data_second) == str:
            last_data_second = last_data_second.replace('年', '-')
            last_data_second = last_data_second.replace('月', '-')
            last_data_second = last_data_second.replace('日', '')
            last_data_second = "\"" + last_data_second + "\""

        return last_data, last_data_second
    else:
        return last_data
