import requests
import threadpool
from retry import retry
import json
import config
import log
import filter


class BaseReq:
    success_count = 0

    def __init__(self,
                 is_json=False,
                 retry_http=3,
                 slience_http_multiplier=2,
                 slience_http_multiplier_max=60,
                 timeout_http=10,
                 post_success_code=201):
        self.is_json = is_json
        self.retry_http = retry_http
        self.slience_http_multiplier = slience_http_multiplier
        self.slience_http_multiplier_max = slience_http_multiplier_max
        self.timeout_http = timeout_http
        self.post_success_code = post_success_code

    # no exception handle, for implement retrying
    def __post_retry(self, post_url, data_list):
        @retry(stop_max_attempt_number=self.retry_http,
               wait_exponential_multiplier=self.slience_http_multiplier*1000,
               wait_exponential_max=self.slience_http_multiplier_max*1000)
        def __post_retry_inner(self,post_url, data_list):
            print("try…… ", end="")
            try:
                if self.is_json:
                    return requests.post(post_url, json=json.dumps(data_list), timeout=self.timeout_http)
                else:
                    return requests.post(post_url, data=data_list, timeout=self.timeout_http)
            except Exception as e:
                print(str(e))
                raise
        return __post_retry_inner(self, post_url, data_list)

    # have exception handle, for implement multi thread
    def post_except(self,post_url, data_list):
        print("post_except:", post_url, " ", )
        try:
            res = self.__post_retry(post_url, data_list)
            if res.status_code == self.post_success_code:
                BaseReq.success_count += 1
            else:
                log.log_error("post data failed\ncode:%d\nresponse:%s\npost_data data:%s"
                              % (res.status_code, "", data_list))
            return res
        except Exception as e:
            log.log_error("server error:" + str(e) + "\ndata:" + str(data_list))


class PostReq(BaseReq):

    # def __init__(self,is_json=False,
    #              retry_http=3,
    #              slience_http_multiplier=2,
    #              slience_http_multiplier_max=60,
    #              timeout_http=10,
    #              post_success_code=201):
    #     super.__init__(self,
    #              is_json=is_json,
    #              retry_http=retry_http,
    #              slience_http_multiplier=slience_http_multiplier,
    #              slience_http_multiplier_max=slience_http_multiplier_max,
    #              timeout_http=timeout_http,
    #              post_success_code=post_success_code)


    # batch post data to webservice
    def post_data(self):
        try:
            if config.enable_thread:  # multi thread
                args = []
                for d in self.data_list:
                    args.append(([self.post_url, d], None))
                pool = threadpool.ThreadPool(config.thread_pool_size)
                reqs = threadpool.makeRequests(self.post_except, args, self.finished_cb)
                [pool.putRequest(req) for req in reqs]
                pool.wait()
                args.clear()
            else:  # single thread
                for d in self.data_list:
                    res = self.post_except(self.post_url, d)
        except Exception:
            raise

    # post success callback
    def finished_cb(*args):
        global success_count
        print("finished_cb  ",args)
        if args[1]:
            print(args[1])
            if args[1].status_code == config.post_success_code:
                success_count += 1
            else:
                log.log_error("post data failed\ncode:%d\nresponse:%s\npost_data data:%s"
                              % (args[1].status_code,args[1].text,args[0]))
        else:
            pass

base_req = PostReq()
base_req.post_except("http://127.0.0.1",{})


class GetReq:

    def __init__(self, get_url):
        self.get_url = get_url

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


    def get_options(table):
        url = config.tables[table]['get_url']
        data = requests.options(url).text
        data = json.loads(data)['actions']['POST']
        return data
        # for i in data:
        #     yield data[i]

    # get_option("stas_date_info")

class Req(PostReq, GetReq):


    def __init__(self,post_url="",data_list=None,get_url=""):
        self.post_url = post_url
        self.data_list = data_list
        self.get_url = get_url
        self.post_req = PostReq(post_url,data_list)
        self.get_req = GetReq(get_url)

req = Req(post_url="127.0.0.1", data_list=[1,2,3])
req.post_data()
