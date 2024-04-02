import os
import logging
import sql_utils
import sql_envs

def config(config_path, meta_info):
    kernel_blacklist = []
    roleType = sql_envs.get_role_type()
    localAccess = sql_envs.get_enable_local_access()
    enableTurboJet = sql_utils.get_enable_turbojet(config_path)
    only_qrs = False
    has_qrs = False
    if localAccess and localAccess.lower() == "true":
        has_qrs = True
        kernel_blacklist = []
    elif roleType == "qrs":
        only_qrs = True
        has_qrs = True
        kernel_blacklist = ["sql.AsyncScanKernel",
                            "sql.KhronosTableScanKernel",
                            "sql.LeftMultiJoinKernel",
                            "sql.LookupJoinKernel",
                            "sql.ScanKernel",
                            "sql.TabletOrcScanKernel"
                            ]
    else:
        kernel_blacklist = ["sql.PlanTransformKernel",
                            "sql.SqlModelOptimizeKernel",
                            "sql.SqlParseKernel",
                            "sql.ClientInfoKernel",
                            "sql.ExternalScanKernel",
                            "sql.ExternalLookupJoinKernel"
                            ]
    if not enableTurboJet:
        kernel_blacklist.append("sql.TurboJetCalcKernel")
    install_root = meta_info["install_root"]
    service_info = meta_info["service_info"]
    zone_name = service_info["zone_name"]
    config = {
        "kernel_list" : [
            "sql\..*",
            "suez_navi\..*",
        ],
        "kernel_blacklist" : kernel_blacklist,
        "resource_config" : [
            {
                "name" : "trace_adapter_r",
                "config" : {
                    "level" : "debug"
                }
            },
            {
                "name" : "ha3_query_info_r",
                "config" : {
                    "query_info" : {
                    }
                }
            },
            {
                "name" : "cava_plugin_manager_r",
                "config" : {}, # sql_utils.get_cava_plugin_manager_config(config_path, zone_name, only_qrs)
            },
            {
                "name" : "cava_jit_wrapper_r",
                "config" : {} #sql_utils.get_cava_config(config_path, zone_name, roleType == "qrs")
            },
            {
                "name" : "query_metric_reporter_r",
                "config" : {
                    "path" : "kmon",
                }
            },
            {
                "name" : "object_pool_r",
                "config" : {
                }
            },
            {
                "name" : "function_interface_creator_r",
                "config" : {}, #(sql_utils.get_function_config(config_path, zone_name) if not only_qrs else {})
            },
            {
                "name" : "sql.table_info_r",
                "config" : ({
                    "zone_name" : zone_name,
                    "inner_docid_optimize_enable" : sql_utils.get_enable_inner_docid_optimize(config_path)
                } if not only_qrs else {})
            },
            {
                "name" : "suez_turing.table_info_r",
                "config" : ({
                    # "item_table" : sql_utils.get_item_table_name_from_sql_config(config_path),
                    "depend_tables" : sql_utils.get_depend_tables_from_sql_config(config_path),
                    # "join_infos" : sql_utils.get_join_infos(config_path, zone_name),
                    "enable_multi_partition" : sql_envs.get_enable_multi_partition()
                } if not only_qrs else {})
            },
            {
                "name" : "partition_info_r",
                "config" : {
                    "part_count" : 1,
                    "part_id" : 0,
                }
            },
            {
                "name" : "suez_turing.cava_allocator_r",
                "config" : {
                }
            },
            {
                "name" : "udf_to_query_manager_r",
                "config" : {
                }
            },
            {
                "name" : "analyzer_factory_r",
                "config" : {
                    "config_path" : config_path
                }
            },
            {
                "name" : "sql.cluster_def_r",
                "config" : {
                }
            },
            {
                "name" : "sql.udf_model_r",
                "config" : {
                    "default_udf" : sql_utils.get_default_udf_function_models(install_root),
                    "zone_udf_map" : {}, #sql_utils.get_zone_function_map(config_path),
                    "special_catalogs" : sql_utils.get_special_catalogs()
                }
            },
            {
                "name" : "normal_scan_r",
                "config" : {
                    "enable_scan_timeout" : sql_utils.get_enable_scan_timeout(config_path)
                }
            },
            {
                "name" : "sql.iquan_r",
                "config" : {
                    "summary_tables" : sql_utils.get_summary_tables(config_path),
                    "table_name_alias" : sql_utils.get_table_name_alias(config_path),
                    "logic_tables" : sql_utils.get_logic_table_config(config_path),
                    "layer_tables" : sql_utils.get_layer_table_config(config_path)
                }
            },
            {
                "name" : "SqlConfigResource",
                "config" : {
                    "iquan_sql_config" : sql_utils.get_sql_config(config_path),
                }
            },
            {
                "name" : "MESSAGE_WRITER_MANAGER_R",
                "config" : ({
                    "swift_writer_config" : sql_utils.get_swift_writer_config(config_path),
                    "table_writer_config" : sql_utils.get_table_writer_config(config_path)
                } if has_qrs else {})
            },
            {
                "name" : "sql.auth_manager_r",
                "config" : ({
                    "authentication_config" : sql_utils.get_auth_config(config_path)
                } if has_qrs else {})
            },
            {
                "name" : "external_table_config_r",
                "config" : (sql_utils.get_external_table_config(config_path) if has_qrs else {})
            }
        ] + sql_utils.get_tvf_resource_config(config_path)  +
        ([
            {
                "name" : "turbojet.catalog_r",
                "config" : {
                    "config_path" : config_path
                }
            }] if enableTurboJet else []),
        "kernel_config": [
            {
                "name" : "sql.format.k",
                "config" : {
                    "format_type" : sql_utils.get_format_type(config_path),
                    "disable_soft_failure" : sql_envs.get_disable_soft_failure()
                }
            }
        ] +
        ([
            {
                "name" : "sql.SqlParseKernel",
                "config" : {
                    "db_name" : sql_utils.get_db_name(config_path),
                    "db_name_alias" : sql_utils.get_db_name_alias(config_path),
                    "enable_turbojet" : enableTurboJet
                }
            }
        ] if has_qrs else [])
    }
    external_gig_config, flow_config_map = sql_utils.get_external_gig_config(config_path)
    if has_qrs:
        flow_config_map.update(sql_utils.get_flow_control_config_from_sql_config(config_path))
    return config, external_gig_config, flow_config_map