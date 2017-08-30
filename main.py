import time
import config
import process


def run():
    while True:
        start_time = time.time()
        process.sync()

        # 保证固定周期时间
        end_time = time.time()
        print("process finised,spend time:", end_time - start_time)
        print('waiting……')
        time.sleep(config.cycle_time - (end_time - start_time))

run()
