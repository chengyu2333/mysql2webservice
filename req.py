import requests
import config
import json


def post(table, data):
    url = config.tables[table]['post_url']
    for d in data:
        data = requests.post(url, d)
        print(data.status_code)
        print(data.text)
        print(d)


def get_last(table=""):
    # data = requests.get_last(url)
    data = '''
    {
        "id": "599ba72fa54d752382002d82",
        "range": "hahaha",
        "seq": 14892,
        "ctime": "2017年02月19日 19:31:59",
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
    return json_data
