import time
import config
import process
import database


def run():
    # database.DB().create_trigger_all()
    while True:
        start_time = time.time()

        # process.sync_api()
        process.sync_trigger()

        # 保证固定周期时间
        end_time = time.time()
        sleep_time = config.cycle_time - (end_time - start_time)
        sleep_time = sleep_time if sleep_time >= 0 else 0
        print("process finised,spend time:", end_time - start_time)
        print('waiting…… %ds' % sleep_time)
        time.sleep(sleep_time)

run()
