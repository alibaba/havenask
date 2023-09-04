import os
import json
import logging
import sql_envs


def get_file_as_json(file_path):
    content = open(file_path).read()
    return json.loads(content, strict=False)


def get_ha3_cluster_map(config_path):
    base_path = os.path.join(config_path, "clusters")
    clusters = os.listdir(base_path)
    cluster_map = {}
    suffix = "_cluster.json"
    for f in clusters:
        file_path = os.path.join(base_path, f)
        if not f.endswith(suffix):
            # logging.info("file ignored: " + file_path)
            continue
        cluster_name = f[:-len(suffix)]
        info = get_file_as_json(os.path.join(base_path, f))
        cluster_map[cluster_name] = info
        # logging.info("add cluster file: " + file_path + ": " + json.dumps(info))
    return cluster_map


def get_zone_list(config_path):
    zones_path = os.path.join(config_path, "zones")
    return os.listdir(zones_path)


def get_default_udf_function_models(binary_path):
    udf_file = os.path.join(binary_path, "usr/local/etc/sql/sql_function.json")
    return get_file_as_json(udf_file)


def zone_name_to_db_name(config_path, zone_name):
    zone_config_path = os.path.join(config_path, "zones/" + zone_name)
    if os.path.exists(os.path.join(zone_config_path, "biz.json")):
        return zone_name
    else:
        return zone_name.split(".")[0]


def get_zone_function(config_path, zone_name):
    func_path = os.path.join(config_path, "sql")
    func_file = os.path.join(func_path, zone_name + "_function.json")
    if (os.path.exists(func_file)):
        return get_file_as_json(func_file)
    else:
        return {}


def get_zone_function_map(config_path):
    db_func = {}
    zone_list = get_zone_list(config_path)
    for zone_name in zone_list:
        db_name = zone_name_to_db_name(config_path, zone_name)
        zone_function = get_zone_function(config_path, zone_name)
        functions_array = []
        if "functions" in zone_function:
            functions_array = zone_function["functions"]
        db_func[db_name] = functions_array
    return db_func


def get_special_catalogs():
    catalog_str = sql_envs.get_special_catalog_list()
    if catalog_str:
        return catalog_str.split(",")
    else:
        return []


def get_zone_config(config_path, zone_name):
    biz_json_file = os.path.join(config_path, "zones", zone_name, "default_biz.json")
    return get_file_as_json(biz_json_file)


def get_table_schema(config_path, table_name):
    schema_json_file = os.path.join(config_path, "schemas", table_name + "_schema.json")
    return get_file_as_json(schema_json_file)


def get_item_table_name(config_path, zone_name):
    zone_config = get_zone_config(config_path, zone_name)
    cluster_config = zone_config["cluster_config"]
    table_name_1 = cluster_config["table_name"]
    schema_config = get_table_schema(config_path, table_name_1)
    return schema_config["table_name"]


def get_join_config(config_path, zone_name):
    zone_config = get_zone_config(config_path, zone_name)
    cluster_config = zone_config["cluster_config"]
    return cluster_config.get("join_config", {})


def get_join_infos(config_path, zone_name):
    join_config = get_join_config(config_path, zone_name)
    return join_config.get("join_infos", [])


def get_turing_options_config(config_path, zone_name):
    zone_config = get_zone_config(config_path, zone_name)
    return zone_config.get("turing_options_config", {})


def get_flow_control_config(config_path):
    flow_control_config_map = {}
    for zone_name in get_zone_list(config_path):
        zone_config = get_zone_config(config_path, zone_name)
        config = zone_config.get("multi_call_config_sql", {})
        if not config:
            continue
        flow_control_config_map[zone_name + ".default_sql"] = config
    return flow_control_config_map


def get_depend_tables(config_path, zone_name):
    zones_path = os.path.join(config_path, "zones")
    if not os.path.exists(zones_path):
        return get_depend_tables_from_sql_config(config_path)

    turing_options = get_turing_options_config(config_path, zone_name)
    if "dependency_table" in turing_options:
        return turing_options["dependency_table"]
    depend_tables = []
    depend_tables.append(zone_name)
    join_config = get_join_config(config_path, zone_name)
    if "join_infos" in join_config:
        join_info = join_config["join_infos"]
        for info in join_info:
            depend_tables.append(info["join_cluster"])
    return depend_tables


def get_sql_config(config_path):
    sql_json_file = os.path.join(config_path, "sql.json")
    if (os.path.exists(sql_json_file)):
        return get_file_as_json(sql_json_file)
    else:
        return {}


def get_enable_inner_docid_optimize(config_path):
    sql_config = get_sql_config(config_path)
    return sql_config.get("inner_docid_optimize_enable", False)


def get_summary_tables(config_path):
    sql_config = get_sql_config(config_path)
    return sql_config.get("summary_tables", [])


def get_table_name_alias(config_path):
    sql_config = get_sql_config(config_path)
    if "table_name_alias" in sql_config:
        return sql_config["table_name_alias"]
    else:
        return {}


def get_db_name(config_path):
    sql_config = get_sql_config(config_path)
    return sql_config.get("db_name", "")


def get_db_name_alias(config_path):
    sql_config = get_sql_config(config_path)
    return sql_config.get("db_name_alias", {})


def get_format_type(config_path):
    sql_config = get_sql_config(config_path)
    if "output_format" in sql_config:
        return sql_config["output_format"]
    else:
        return ""


def get_swift_writer_config(config_path):
    sql_config = get_sql_config(config_path)
    if "swift_writer_config" in sql_config:
        return sql_config["swift_writer_config"]
    else:
        return {"table_read_write_config": {}}


def get_table_writer_config(config_path):
    sql_config = get_sql_config(config_path)
    if "table_writer_config" in sql_config:
        return sql_config["table_writer_config"]
    else:
        return {"zone_names": []}


def get_function_config(config_path, zone_name):
    zone_config = get_zone_config(config_path, zone_name)
    if "function_config" in zone_config:
        function_config = zone_config["function_config"]
        if "cava_functions" in function_config:
            del function_config["cava_functions"]
        return function_config
    else:
        return {}


def get_function_plugins(config_path):
    so_list = []
    for zone_name in get_zone_list(config_path):
        func_config = get_function_config(config_path, zone_name)
        if "modules" not in func_config:
            continue
        modules = func_config["modules"]
        for module in modules:
            so_list.append(get_module_abs_path(config_path, module["module_path"]))
    return list(set(so_list))


def get_auth_config(config_path):
    sql_config = get_sql_config(config_path)
    return sql_config.get("authentication_config", {})


def get_qrs_config(config_path):
    qrs_json_file = os.path.join(config_path, "qrs.json")
    if (os.path.exists(qrs_json_file)):
        return get_file_as_json(qrs_json_file)
    else:
        return {}


def get_qrs_cava_function_infos(config_path):
    json_info = {}
    json_file = os.path.join(config_path, "qrs_func.json")
    if (os.path.exists(json_file)):
        json_info = get_file_as_json(json_file)
    return json_info.get("cava_functions", [])


def get_qrs_cava_config(config_path):
    biz_json_file = os.path.join(config_path, "biz.json")
    if (os.path.exists(biz_json_file)):
        biz_config = get_file_as_json(biz_json_file)
        if "cava_config" in biz_config:
            return biz_config["cava_config"]
    qrs_config = get_qrs_config(config_path)
    return qrs_config.get("cava_config", {})


def get_cava_plugin_manager_config(config_path, zone_name, only_qrs):
    if only_qrs:
        return get_qrs_cava_plugin_manager_config(config_path)
    else:
        return get_searcher_cava_plugin_manager_config(config_path, zone_name)


def get_qrs_cava_plugin_manager_config(config_path):
    auto_register = []
    cava_config = get_qrs_cava_config(config_path)
    if "auto_register_function_packages" in cava_config:
        auto_register = cava_config["auto_register_function_packages"]
    return {
        "function_infos": get_qrs_cava_function_infos(config_path),
        "auto_register_function_packages": auto_register
    }


def get_searcher_cava_plugin_manager_config(config_path, zone_name):
    auto_register = []
    infos = []
    zone_config = get_zone_config(config_path, zone_name)
    if "function_config" in zone_config:
        function_config = zone_config["function_config"]
        if "cava_functions" in function_config:
            infos += function_config["cava_functions"]
    if "cava_config" in zone_config:
        cava_config = zone_config["cava_config"]
        if "auto_register_function_packages" in cava_config:
            auto_register = cava_config["auto_register_function_packages"]
    return {
        "function_infos": infos,
        "auto_register_function_packages": auto_register
    }


def get_cava_config(config_path, zone_name, is_qrs):
    config = {}
    if not is_qrs:
        zone_config = get_zone_config(config_path, zone_name)
        if "cava_config" in zone_config:
            config = zone_config["cava_config"]
    else:
        config = get_qrs_cava_config(config_path)
    config["cava_conf"] = "../binary/usr/local/etc/sql/sql_cava_config.json"
    return {
        "config_path": config_path,
        "cava_config": config
    }


def get_logic_table_config(config_path):
    tables = []
    base_path = os.path.join(config_path, "sql")
    if not os.path.exists(base_path):
        return {"tables": []}
    logic_files = os.listdir(base_path)
    suffix = "_logictable.json"
    for f in logic_files:
        file_path = os.path.join(base_path, f)
        if not f.endswith(suffix):
            # logging.info("file ignored: " + file_path)
            continue
        db_name = f[:-len(suffix)]
        info = get_file_as_json(os.path.join(base_path, f))
        if "tables" in info:
            tables += info["tables"]
        # logging.info("add logic table file: " + file_path + ": " + json.dumps(info))
    return {"tables": tables}


def get_layer_table_config(config_path):
    tables = []
    base_path = os.path.join(config_path, "sql")
    if not os.path.exists(base_path):
        return []
    logic_files = os.listdir(base_path)
    suffix = "_layer_table.json"
    for f in logic_files:
        file_path = os.path.join(base_path, f)
        if not f.endswith(suffix):
            # logging.info("file ignored: " + file_path)
            continue
        db_name = f[:-len(suffix)]
        info = get_file_as_json(os.path.join(base_path, f))
        if "layer_tables" in info:
            tables += info["layer_tables"]
        # logging.info("add layer table file: " + file_path + ": " + json.dumps(info))
    return tables


def get_agg_plugins(config_path):
    sql_config = get_sql_config(config_path)
    so_list = []
    if "sql_agg_plugin_config" in sql_config:
        agg_config = sql_config["sql_agg_plugin_config"]
        if "modules" in agg_config:
            modules = agg_config["modules"]
            for module in modules:
                if "module_path" in module:
                    so_list.append(get_module_abs_path(config_path, module["module_path"]))
    return so_list


def get_tvf_plugins(config_path):
    sql_config = get_sql_config(config_path)
    so_list = []
    if "sql_tvf_plugin_config" in sql_config:
        tvf_config = sql_config["sql_tvf_plugin_config"]
        if "modules" in tvf_config:
            modules = tvf_config["modules"]
            for module in modules:
                if "module_path" in module:
                    so_list.append(get_module_abs_path(config_path, module["module_path"]))
    return so_list


def get_module_abs_path(config_path, module_path):
    plugin_path = os.path.join(config_path, "plugins", module_path)
    if os.path.exists(plugin_path):
        return plugin_path
    else:
        return module_path


def get_tvf_plugin_config(config_path):
    sql_config = get_sql_config(config_path)
    if "sql_tvf_plugin_config" in sql_config:
        tvf_config = sql_config["sql_tvf_plugin_config"]
        tvf_config["config_path"] = config_path
        return tvf_config
    return {
        "config_path": config_path,
    }


def get_tvf_resource_config(config_path):
    tvf_plugin_config = get_tvf_plugin_config(config_path)
    if "tvf_profiles" not in tvf_plugin_config:
        return []
    profiles = tvf_plugin_config["tvf_profiles"]
    config_map = {}
    for profile in profiles:
        if "func_name" not in profile:
            continue
        func_name = profile["func_name"]
        if "tvf_name" not in profile:
            continue
        if func_name not in config_map:
            config_map[func_name] = [profile]
        else:
            config_map[func_name].append(profile)
    resource_config = []
    for func, profiles in config_map.items():
        resource_config.append({
            "name": func,
            "config": {
                "config_path": config_path,
                "tvf_profiles": profiles
            }
        })
    return resource_config


def get_enable_turbojet(config_path):
    sql_config = get_sql_config(config_path)
    if "enable_turbojet" in sql_config:
        return sql_config["enable_turbojet"]
    else:
        return False


def get_enable_scan_timeout(config_path):
    sql_config = get_sql_config(config_path)
    return sql_config.get("enable_scan_timeout", True)


def get_external_table_config(config_path):
    sql_config = get_sql_config(config_path)
    return sql_config.get("external_table_config", {})


def get_external_gig_config(config_path):
    external_config = get_external_table_config(config_path)
    gig_config = external_config.get("gig_config", {})
    return gig_config.get("subscribe", {}), gig_config.get("flow_control", {})


def get_flow_control_config_from_sql_config(config_path):
    sql_config = get_sql_config(config_path)
    flow_control_config_map = {}
    flow_config = sql_config.get("multi_call_config_sql", {})
    for zone_name, config in flow_config.items():
        flow_control_config_map[zone_name + ".default_sql"] = config
    return flow_control_config_map


def get_depend_tables_from_sql_config(config_path):
    sql_config = get_sql_config(config_path)
    depend_tables = sql_config.get('depend_tables', [])
    return depend_tables


def get_item_table_name_from_sql_config(config_path):
    sql_config = get_sql_config(config_path)
    item_table_name = sql_config.get('item_table', '')
    return item_table_name
