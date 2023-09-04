def config(config_path, meta_info, biz_name, conf):
    return {
        "publish_infos" : {
            "biz_alias_names" : [
                biz_name,
                biz_name + "_alias_1",
                biz_name + "_alias_2",
            ],
            "version" : 3,
        },
        "kernel_list" : [
            ".*",
        ],
        "init_resource" : [
            "A",
            "D",
            "H",
            "test_static_graph_loader",
        ],
        "resource_config" : [
            {
                "name" : "H",
                "config" : {
                },
                "dynamic_resource" : [
                    "A",
                    "B",
                ]
            }
        ],
        "graph_config" : {
            "graph_a" : {
                "graph_file" : "graph.serialize",
                "resource_config" : [
                    {
                        "name" : "A",
                        "config" : {
                        },
                        "dynamic_resource" : [
                        ]
                    },
                    {
                        "name" : "resource2",
                        "config" : {
                        }
                    }
                ]
            }
        }
    }
