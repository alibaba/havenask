import json
import os
import logging
import entry_loader
import sys
import sql_envs
import sql_utils
import copy

sys.path.append(os.path.dirname(os.path.realpath(__file__)))

def load(config_path, load_param):
    param_map = json.loads(load_param)
    fill_local_config_path(param_map)
    # logging.info("param_map: " + json.dumps(param_map, indent = 4))
    biz_metas = param_map["biz_metas"]
    service_info = param_map["service_info"]
    custom_app_info = param_map["custom_app_info"]
    module_list = []
    for biz_name, conf in biz_metas.items():
        config_path = conf["config_path"];
        module_list += get_module_array(config_path)
    biz_config, biz_flow_config_map, gig_biz_sub_configs = get_biz_config(biz_metas, param_map)
    config = {
        "version" : 0,
        "log_config" : get_log_config(),
        "engine_config" : get_engine_config(),
        "modules" : module_list,
        "biz_config" : biz_config,
        "resource_config" : [
        ],
        "resource_list" : [
            "suez_navi.health_check_rpc_r",
            "sql.sql_rpc.r",
        ],
        "sleep_before_update_us" : 0 * 1000 * 1000,
    }
    gig_config = {
        "name" : "navi.buildin.gig_client",
        "config" : get_gig_config(service_info, gig_biz_sub_configs, biz_flow_config_map),
    }
    config["resource_config"].append(gig_config)
    health_check_part_info_map = {}
    for biz_name, conf in biz_config.items():
        health_check_part_info_map[biz_name] = {
            "part_count" : conf["part_count"],
            "part_ids" : conf["part_ids"]
        }
    health_check_config = {
        "name" : "suez_navi.health_check_rpc_r",
        "config" : {
            "kernel_name" : "suez_navi.health_check_k",
            "cm2_http_alias" : [
                "/SearchService/cm2_status",
                "/SearchService/vip_status",
                "/status_check",
                "/QrsService/cm2_status",
            ],
            "biz_part_info_map" : health_check_part_info_map
        }
    }
    config["resource_config"].append(health_check_config)
    return config

def fill_local_config_path(param_map):
    biz_metas = param_map["biz_metas"]
    biz_local_config_paths = param_map["biz_local_config_paths"]
    for biz_name, conf in biz_metas.items():
        remote_config_path = conf["config_path"]
        conf["config_path"] = biz_local_config_paths.get(biz_name, remote_config_path)

def get_log_config():
    return {
        "file_appenders" : [
            {
                "file_name" : "logs/navi.log",
                "log_level" : "info",
                "bt_filters" : [
                ]
            }
        ]
    }

def get_engine_config():
    return {
        "thread_num" : 0,
        "queue_size" : 1000,
    }

def get_module_array(config_path):
    agg = sql_utils.get_agg_plugins(config_path)
    tvf = sql_utils.get_tvf_plugins(config_path)
    return list(set(agg + tvf))

def get_biz_config(biz_metas, param_map):
    table_part_info = param_map["table_part_info"]
    service_info = param_map["service_info"]
    part_count = service_info["part_count"]
    part_id = service_info["part_id"]
    zone_name = service_info["zone_name"]
    is_qrs = (sql_envs.get_role_type() == "qrs")
    biz_config_map = {}
    flow_config_map = {}
    biz_gig_config_list = []
    for biz_name, conf in biz_metas.items():
        biz_param_map = copy.deepcopy(param_map)
        biz_name = get_real_biz_name(zone_name, is_qrs)
        biz_param_map["biz_name"] = biz_name
        config_path = conf["config_path"]
        max_part_count, max_part_ids = get_table_max_part_count(is_qrs, config_path, zone_name, table_part_info)
        navi_biz_conf = {}
        biz_gig_config = {}
        biz_flow_config_map = {}
        if os.path.exists(os.path.join(config_path, "navi.py")):
            navi_biz_conf, biz_gig_config, biz_flow_config_map = entry_loader.load(config_path, biz_param_map)
        else:
            default_script = os.path.join(biz_param_map["install_root"], "sql_default.py")
            navi_biz_conf, biz_gig_config, biz_flow_config_map = entry_loader.load(config_path, biz_param_map, default_script)
        navi_biz_conf["config_path"] = config_path
        navi_biz_conf["part_count"] = max_part_count
        navi_biz_conf["part_ids"] = max_part_ids
        biz_config_map[biz_name] = navi_biz_conf
        if biz_flow_config_map.keys() & flow_config_map.keys():
            raise Exception("flow control config key conflict, biz map: [%s], all [%s]" %
                            (json.dumps(biz_flow_config_map, indent = 4), json.dumps(flow_config_map, indent = 4)))
        flow_config_map.update(biz_flow_config_map)
        biz_gig_config_list.append(biz_gig_config)
    return biz_config_map, flow_config_map, biz_gig_config_list

def get_table_max_part_count(is_qrs, config_path, zone_name, table_part_info):
    if is_qrs:
        return 1, [0]
    depend_tables = sql_utils.get_depend_tables(config_path, zone_name)

    max_part_count = 0
    if 0 == len(depend_tables):
        max_table_name = None
        for table_name, part_info in table_part_info.items():
            part_count = part_info["part_count"]
            if part_count > max_part_count:
                max_part_count = part_count
                max_table_name = table_name
    else:
        max_table_name = None
        for table_name in depend_tables:
            part_info = table_part_info[table_name]
            part_count = part_info["part_count"]
            if part_count > max_part_count:
                max_part_count = part_count
                max_table_name = table_name

    if max_table_name is None:
        return 1, [0]
    max_part_ids =  table_part_info[max_table_name]["part_ids"]
    return max_part_count, max_part_ids

def get_real_biz_name(zone_name, is_qrs):
    if is_qrs:
        return "qrs.default_sql"
    else:
        return zone_name + ".default_sql"

def get_cm2_config_array(service_info):
    cm2_config = []
    if "sub_cm2_configs" in service_info:
        if len(service_info["sub_cm2_configs"]) > 0:
            cm2_config = service_info["sub_cm2_configs"]

    if len(cm2_config) == 0:
        if "cm2_config" in service_info:
            if len(service_info["cm2_config"]) > 0:
                cm2_config.append(service_info["cm2_config"])
    return cm2_config

def add_cm2_config(cm2_config_map, zk_host, zk_path, clusters):
    serverKey = zk_host + zk_path
    if not serverKey in cm2_config_map:
        cm2_config_map[serverKey] = {}
    cm2_config_map[serverKey]["zk_host"] = zk_host
    cm2_config_map[serverKey]["zk_path"] = zk_path
    if not "cm2_part" in cm2_config_map[serverKey]:
        cm2_config_map[serverKey]["cm2_part"] = clusters
    else:
        cm2_config_map[serverKey]["cm2_part"] += clusters

def get_gig_config(service_info, gig_biz_sub_configs, biz_flow_config_map):
    cm2_config_array = get_cm2_config_array(service_info)
    for biz_gig_config in gig_biz_sub_configs:
        if "cm2" in biz_gig_config:
            cm2_config_array.append(biz_gig_config["cm2"])
        if "cm2_configs" in biz_gig_config:
            cm2_config_array += biz_gig_config["cm2_configs"]
        if "istio" in biz_gig_config:
            cm2_config_array.append(biz_gig_config["istio"])
        if "istio_configs" in biz_gig_config:
            cm2_config_array += biz_gig_config["istio_configs"]
        if "local" in biz_gig_config:
            cm2_config_array.append({
                "local" : biz_gig_config["local"]
            })
    cm2_config_map = {}
    istios_subs = []
    local_subs = []
    cm2_subs = []
    for config in cm2_config_array:
        if "istio_host" in config:
            istios_subs.append(config)
        if "local" in config:
            local_subs += config["local"]
        if "cm2_server_cluster_name" in config:
            zk_host = config["cm2_server_zookeeper_host"]
            zk_path = config["cm2_server_leader_path"]
            clusters = config["cm2_server_cluster_name"].split(",")
            add_cm2_config(cm2_config_map, zk_host, zk_path, clusters)
        if "zk_host" in config:
            zk_host = config["zk_host"]
            zk_path = config["zk_path"]
            clusters = config["cm2_part"]
            add_cm2_config(cm2_config_map, zk_host, zk_path, clusters)
    cm2_subs = list(cm2_config_map.values())
    sub_config = {}
    if len(cm2_subs) > 0:
        sub_config["cm2_configs"] = cm2_subs
    if len(local_subs) > 0:
        sub_config["local"] = local_subs
    if len(istios_subs) > 0:
        sub_config["istio_configs"] = istios_subs
    # sub_config["allow_empty_sub"] = False
    return {
        "init_config" : {
            "subscribe_config" : sub_config,
            "misc_config" : {
            },
            "connection_config" : {
                "grpc_stream" : {
                    "thread_num" : 20,
                    "queue_size": 1000,
                },
                "arpc" : {
                    "thread_num" : 20,
                    "queue_size": 1000,
                }
            }
        },
        "flow_config" : biz_flow_config_map
    }
