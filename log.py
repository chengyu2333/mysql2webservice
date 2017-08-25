import io
import time
import config


def log_error(msg):
    f = io.open("log/" + time.strftime("%Y-%m-%d", time.localtime()) + " errors.log", 'a', encoding="utf-8")
    log = time.ctime() + "\t" + msg + "\r\n"
    if config.print_log:
        print("\033[1;31m" + log + "\033[0m!")
    f.write(log)
    f.close()


def log_success(msg):
    f = io.open("log/" + time.strftime("%Y-%m-%d", time.localtime()) + " success.log", 'a', encoding="utf-8")
    log = time.ctime() + "\t" + msg + "\r\n"
    if config.print_log:
        print(log)
    f.write(log)
    f.close()
