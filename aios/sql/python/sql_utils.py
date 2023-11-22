import os
import json
import logging
import sql_envs


def get_file_as_json(file_path):
    if not os.path.exists(file_path):
        return {}
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
            continue
        cluster_name = f[:-len(suffix)]
        info = get_file_as_json(os.path.join(base_path, f))
        cluster_map[cluster_name] = info
    return cluster_map


def get_zone_list(config_path):
    zones_path = os.path.join(config_path, "zones")
    if not os.path.exists(zones_path):
        return []
    return os.listdir(zones_path)


def get_default_udf_function_models(binary_path):
    udf_file = os.path.join(binary_path, "usr/local/etc/sql/sql_function.json")
    info = get_file_as_json(udf_file)
    return info.get("functions", [])


def get_system_udf_function_models(config_path):
    system_function = get_db_function(config_path, "system")
    return system_function.get("functions", [])


def zone_name_to_db_name(config_path, zone_name):
    zone_config_path = os.path.join(config_path, "zones/" + zone_name)
    if os.path.exists(os.path.join(zone_config_path, "biz.json")):
        return zone_name
    else:
        return zone_name.split(".")[0]


def get_db_function(config_path, db_name):
    func_path = os.path.join(config_path, "sql")
    func_file = os.path.join(func_path, db_name + "_function.json")
    return get_file_as_json(func_file)


def get_db_function_map(config_path):
    db_func = {}
    zone_list = get_zone_list(config_path)
    for zone_name in zone_list:
        db_name = zone_name_to_db_name(config_path, zone_name)
        zone_function = get_db_function(config_path, db_name)
        functions_array = zone_function.get("functions", [])
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
    cluster_config = zone_config.get("cluster_config", {})
    table_name_1 = cluster_config.get("table_name", "")
    schema_config = get_table_schema(config_path, table_name_1)
    return schema_config.get("table_name", "")


def get_join_config(config_path, zone_name):
    zone_config = get_zone_config(config_path, zone_name)
    cluster_config = zone_config.get("cluster_config", {})
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
    return turing_options.get("dependency_table", [])


def get_sql_config(config_path):
    sql_json_file = os.path.join(config_path, "sql.json")
    if (os.path.exists(sql_json_file)):
        d = get_file_as_json(sql_json_file)
        lack_result_enable = d.pop('lack_result_enable', None)
        result_allow_soft_failure = d.pop('result_allow_soft_failure', lack_result_enable)
        if result_allow_soft_failure is not None:
            d['result_allow_soft_failure'] = result_allow_soft_failure
        return d
    else:
        return {}


def get_iquan_jni_config(config_path):
    sql_config = get_sql_config(config_path)
    return sql_config.get("iquan_jni_config", {})


def get_iquan_client_config(config_path):
    sql_config = get_sql_config(config_path)
    return sql_config.get("iquan_client_config", {})


def get_iquan_warmup_config(config_path):
    sql_config = get_sql_config(config_path)
    warmup_config = sql_config.get("iquan_warmup_config", {})
    if "warmup_file_path" in warmup_config and warmup_config["warmup_file_path"] != "":
        warmup_config["warmup_file_path"] = os.path.join(config_path, warmup_config["warmup_file_path"])
    return warmup_config


def get_enable_inner_docid_optimize(config_path):
    sql_config = get_sql_config(config_path)
    return sql_config.get("inner_docid_optimize_enable", False)


def get_db_name(config_path):
    sql_config = get_sql_config(config_path)
    return sql_config.get("db_name", "")


def get_db_name_alias(config_path):
    sql_config = get_sql_config(config_path)
    return sql_config.get("db_name_alias", {})


def get_format_type(config_path):
    sql_config = get_sql_config(config_path)
    return sql_config.get("output_format", "")


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


def get_one_function_config(config_path, f):
    one_function_file = os.path.join(config_path, f)
    one_function_map = get_file_as_json(one_function_file)
    if "functions" in one_function_map:
        return one_function_map["functions"]
    else:
        return []


def get_function_config(config_path, zone_name):
    zone_config = get_zone_config(config_path, zone_name)
    if "function_config" in zone_config:
        function_config = zone_config["function_config"]
        if "cava_functions" in function_config:
            del function_config["cava_functions"]
        function_config["config_path"] = config_path
        if "modules" in function_config:
            one_function_configs = []
            for module in function_config["modules"]:
                if "parameters" in module:
                    parameters = module["parameters"]
                    if "config_path" in parameters:
                        one_function_configs += get_one_function_config(config_path, parameters["config_path"])
            if "functions" in function_config:
                function_config["functions"] += one_function_configs
            else:
                function_config["functions"] = one_function_configs
            del function_config["modules"]
        return function_config
    else:
        return {}


def get_function_plugins(config_path, load_zone_name):
    so_list = []
    for zone_name in get_zone_list(config_path):
        if load_zone_name != "" and zone_name != load_zone_name:
            continue
        zone_config = get_zone_config(config_path, zone_name)
        func_config = zone_config.get("function_config", {})
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
    return get_file_as_json(qrs_json_file)


def get_qrs_cava_function_infos(config_path):
    json_info = {}
    json_file = os.path.join(config_path, "qrs_func.json")
    json_info = get_file_as_json(json_file)
    return json_info.get("cava_functions", [])


def get_qrs_cava_config(config_path):
    biz_json_file = os.path.join(config_path, "biz.json")
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
    tables = {}
    base_path = os.path.join(config_path, "sql")
    if not os.path.exists(base_path):
        return tables
    files = os.listdir(base_path)
    suffix = "_logictable.json"
    for f in files:
        file_path = os.path.join(base_path, f)
        if not f.endswith(suffix):
            continue
        db_name = f[:-len(suffix)]
        info = get_file_as_json(os.path.join(base_path, f))
        if "tables" in info:
            tables[db_name] = info["tables"]
        logging.info("add logic table file: " + file_path + ", db: " + db_name + ": " + json.dumps(info))
    return tables


def get_layer_table_config(config_path):
    tables = {}
    base_path = os.path.join(config_path, "sql")
    if not os.path.exists(base_path):
        return tables
    files = os.listdir(base_path)
    suffix = "_layer_table.json"
    for f in files:
        file_path = os.path.join(base_path, f)
        if not f.endswith(suffix):
            continue
        db_name = f[:-len(suffix)]
        info = get_file_as_json(os.path.join(base_path, f))
        if "layer_tables" in info:
            tables[db_name] = info["layer_tables"]
        logging.info("add layer table file: " + file_path + "db: " + db_name + ": " + json.dumps(info))
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
    return sql_config.get("enable_turbojet", False)


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
    return sql_config.get('depend_tables', [])


def get_item_table_name_from_sql_config(config_path):
    sql_config = get_sql_config(config_path)
    return sql_config.get('item_table', '')
