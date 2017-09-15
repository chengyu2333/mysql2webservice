import req
import config
import process

req = req.Req()
url  = config.tables["stas_date_info"]['get_url']
# req.get_last_flag(url,"ctime","seq")
# r = req.get_options(url)
# print(r)
# req.post_data("http://127.0.0.1", [{},{},{}], enable_thread=False)

process.sync_by_api()
process.sync_by_trigger()