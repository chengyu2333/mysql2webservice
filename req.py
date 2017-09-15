import requests
import threadpool
from retry import retry
import json
import config
import log
import filter

#TODO class ReqHeader
class BaseReq:
    def __init__(self,
                 retry_http=3,
                 slience_http_multiplier=2,
                 slience_http_multiplier_max=60,
                 timeout_http=10):
        self.success_count = 0
        self.retry_http = retry_http
        self.slience_http_multiplier = slience_http_multiplier
        self.slience_http_multiplier_max = slience_http_multiplier_max
        self.timeout_http = timeout_http

    # try post data
    def post_try(self, post_url, data_list, post_json=False, callback=None):
        print("post:", post_url, "try ", end="")

        @retry(stop_max_attempt_number=self.retry_http,
               wait_exponential_multiplier=self.slience_http_multiplier*1000,
               wait_exponential_max=self.slience_http_multiplier_max*1000)
        def __post_retry():
            print(".", end="")
            try:
                if post_json:
                    return requests.post(post_url, json=json.dumps(data_list), timeout=self.timeout_http)
                else:
                    return requests.post(post_url, data=data_list, timeout=self.timeout_http)
            except Exception as e:
                print(str(e))
                raise
        response = __post_retry()
        print()
        if callback:callback(response)
        else:self.__callback(response)
        return response

    # get request
    def get_try(self, get_url, callback=None):
        print("get:", get_url, "try ", end="")
        @retry(stop_max_attempt_number=self.retry_http,
               wait_exponential_multiplier=self.slience_http_multiplier*1000,
               wait_exponential_max=self.slience_http_multiplier_max*1000)
        def get_retry_inner():
            print(".", end="")
            try:
                return requests.get(get_url)
            except Exception as e:
                print(str(e))
                raise
        response = get_retry_inner()
        print()
        if callback:callback(response)
        else:self.__callback(response)
        return response

    # request success callback
    def __callback(self, *args):
        print(args)


class PostReq(BaseReq):
    # batch post data to webservice
    def post_data(self, post_url, data_list, post_json=False, enable_thread=False,thread_pool_size=10,post_success_code=201):
        self.post_success_code = post_success_code
        try:
            if enable_thread:
                args = []
                for data in data_list:
                    args.append(([post_url, data, post_json, self.__callback], None))
                print("args",args)
                pool = threadpool.ThreadPool(thread_pool_size)
                reqs = threadpool.makeRequests(self.post_try, args)
                [pool.putRequest(req) for req in reqs]
                pool.wait()
                args.clear()
            else:
                for d in data_list:
                    self.post_try(post_url, d, post_json, self.__callback)
        except Exception:
            raise

    def __callback(self, *args):
        response = args[0]
        if response:
            if response.status_code == self.post_success_code:
                self.success_count += 1
                print("success")
            else:
                log.log_error("post data failed\ncode:%d\nresponse:%s\npost_data data:%s"
                              % (response.status_code,response,response))


class GetReq(BaseReq):
    @staticmethod
    def get_options(url):
        data = requests.options(url).text
        data = json.loads(data)['actions']['POST']
        return data

    # get last flag from webservice
    def get_last_flag(self, url, cmp_arg, cmp_arg_second=""):
        data = self.get_try(url)
        test_data = '''
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

        if data.status_code==200:
            json_data = json.loads(data.text)
            json_data = json.loads(test_data)

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


class Req(PostReq, GetReq):
    pass