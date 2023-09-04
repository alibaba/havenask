def config(config_path, meta_info):
    return {
        "kernel_list" : [
            ".*",
        ],
        "resource_config" : [],
        "graph_config" : {
            "graph_a" : {
                "graph_file" : "graph.serialize"
            }
        }
    }
