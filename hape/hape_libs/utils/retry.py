from hape_libs.utils.logger import Logger
import time

class Retry:
    @staticmethod
    def retry(func, check_msg, limit=20, sleep_secs=5):
        max_time = limit * sleep_secs
        Logger.info("Begin to check [{}]".format(check_msg))
        for retry in range(limit):
            if not func():
                Logger.info("Check [{}] not succeed by now, retry {}/{}, wait up to {} seconds".format(check_msg, retry, limit, max_time))
            else:
                Logger.info("Check [{}] succeed".format(check_msg))
                return True
            time.sleep(sleep_secs)
        Logger.warning("Check [{}] timeout".format(check_msg))
        return False
