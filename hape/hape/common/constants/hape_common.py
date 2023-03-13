import os
class HapeCommon:
    ROLES = ["bs", "searcher", "qrs"]
    ROLE_BIZ_PLAN_KEY = {
        "bs": "index_config",
        "searcher": "searcher_config",
        "qrs": "qrs_config"
    }
    ADMIN_DOMAIN_DIR = "domains"
    PROCESSOR_INFO_KEY = "processor_info"
    HOSTIPS_KEY = "host_ips"
    HB_WORKDIR = "heartbeats"
    PACKAGE_KEY = "packages"
    HEARTBEAT_TIMEOUT_SEC = 180
    
    
    # @staticmethod
    # def parse_biz_plan(role_name, plan):
    #     biz_plan_key = HapeCommon.ROLE_BIZ_PLAN_KEY[role_name]
    #     if role_name == "bs":
    #         return plan[biz_plan_key]
    #     else:
    #         return plan[biz_plan_key]

