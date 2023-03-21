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
    
    START_WORKER_COMMAND = "start-worker"
    STOP_WORKER_COMMAND = "stop-worker"
    REMOVE_WORKER_COMMAND = "remove-worker"
    DP_COMMAND = "dp"
    UPC_COMMAND = "upc"
    UPF_COMMAND = "upf"
    
    LAST_TARGET_TIMESTAMP_KEY = {
        "final-target": "last-final-target-timestamp",
        "user-target": "last-user-target-timestamp",
        "heartbeat": "last-heartbeat-timestamp"
    }

