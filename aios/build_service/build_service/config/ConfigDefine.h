#ifndef ISEARCH_BS_CONFIGDEFINE_H
#define ISEARCH_BS_CONFIGDEFINE_H

#include <string>

namespace build_service {
namespace config {

static const std::string SCHEMA_CONFIG_PATH = "schemas";
static const std::string SCHEMA_JSON_FILE_SUFFIX = "_schema.json";
static const std::string CLUSTER_CONFIG_PATH = "clusters";
static const std::string CLUSTER_JSON_FILE_SUFFIX = "_cluster.json";
static const std::string LUA_CONFIG_PATH = "lua_scripts";
static const std::string DATA_TABLE_PATH = "data_tables";
static const std::string DATA_TABLE_JSON_FILE_SUFFIX = "_table.json";
static const std::string TASK_SCRIPTS_CONFIG_PATH = "task_scripts";

static const std::string BUILD_APP_FILE_NAME = "build_app.json";
static const std::string HIPPO_FILE_NAME = "bs_hippo.json";
static const std::string MODIFIED_FIELDS_DOCUMENT_PROCESSOR_NAME = "ModifiedFieldsDocumentProcessor";

static const std::string INHERIT_FROM_KEY = "inherit_from";
static const std::string INHERIT_FROM_EXTERNAL_KEY = "inherit_from_external";

static const std::string BS_FIELD_SEPARATOR = "field_separator";
static const std::string BS_FILE_NAME = "file_name";
static const std::string BS_LINE_DATA = "line_data";
static const std::string BS_ALTER_FIELD_MERGE_CONFIG = "alter_field";
static const std::string DEFAULT_ALTER_FIELD_MERGER_NAME = "AlterDefaultFieldMerger";
static const std::string TASK_CONFIG_SUFFIX = "_task.json";
static const int64_t INVALID_SCHEMAVERSION = -1;
static const std::string BS_TABLE_META_SYNCHRONIZER = "table_meta_synchronizer";
static const std::string BS_TASK_DOC_RECLAIM = "doc_reclaim_task";
static const std::string BS_TASK_END_BUILD = "end_build_task";
static const std::string BS_TASK_CLONE_INDEX = "clone_index_task";
static const std::string BS_REPARTITION = "repartition";
static const std::string BS_TASK_RUN_SCRIPT = "run_script";
static const std::string BS_SNAPSHOT_VERSION = "snapshot_version";
static const std::string BS_TASK_CONFIG_FILE_PATH = "task_config_file_path";
static const std::string BS_DROP_BUILDING_INDEX = "drop_building_index";

static const std::string BS_ENDBUILD_VERSION = "endbuild_version";
static const std::string BS_ENDBUILD_PARTITION_COUNT = "endbuild_partition_count";
static const std::string BS_ENDBUILD_WORKER_PATHVERSION = "endbuild_worker_pathversion";

static const std::string BS_ROLLBACK_SOURCE_VERSION = "rollback_source_version";
static const std::string BS_ROLLBACK_TARGET_VERSION = "rollback_target_version";

// builtin task

static const std::string BS_EXTRACT_DOC = "extract_doc";
static const std::string BS_EXTRACT_DOC_CHECKPOINT = "extract_doc_checkpoint";
static const std::string BS_PREPARE_DATA_SOURCE = "prepare_data_source";
static const std::string BS_ROLLBACK_INDEX = "rollback_index";
static const std::string BS_ENDBUILD = "endbuild";
static const std::string BS_BUILDIN_BUILD_SERVICE_TASK_PREFIX = "libbuild_service_task.so";
static const std::string BS_DOC_RECLAIM_CONF_FILE = "reclaim_config";

static const int32_t UNKNOWN_WORKER_PROTOCOL_VERSION = -1;

}
}

#endif //ISEARCH_BS_CONFIGDEFINE_H
