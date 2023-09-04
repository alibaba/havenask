/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "build_service/config/ConfigDefine.h"

namespace build_service { namespace config {

const std::string SCHEMA_CONFIG_PATH = "schemas";
const std::string SCHEMA_JSON_FILE_SUFFIX = "_schema.json";
const std::string SCHEMA_PATCH_JSON_FILE_SUFFIX = "_schema_patch.json";
const std::string CLUSTER_CONFIG_PATH = "clusters";
const std::string PROCESSOR_CONFIG_PATH = "processors";
const std::string CLUSTER_JSON_FILE_SUFFIX = "_cluster.json";
const std::string LUA_CONFIG_PATH = "lua_scripts";
const std::string GRAPH_CONFIG_FILE = "graph.json";
const std::string AGENT_GROUP_CONFIG_FILE = "agent_group.json";
const std::string RAW_DOC_FUNCTION_RESOURCE_FILE = "doc_query_function.json";
const std::string DATA_TABLE_PATH = "data_tables";
const std::string DATA_TABLE_JSON_FILE_SUFFIX = "_table.json";
const std::string TASK_SCRIPTS_CONFIG_PATH = "task_scripts";

const std::string BUILD_APP_FILE_NAME = "build_app.json";
const std::string HIPPO_FILE_NAME = "bs_hippo.json";

const std::string INHERIT_FROM_KEY = "inherit_from";
const std::string INHERIT_FROM_EXTERNAL_KEY = "inherit_from_external";
const std::string BS_APP_NAME = "appName";
const std::string BS_FIELD_SEPARATOR = "field_separator";
const std::string BS_FILE_NAME = "file_name";
const std::string BS_LINE_DATA = "line_data";
const std::string BS_ALTER_FIELD_MERGE_CONFIG = "alter_field";
const std::string BS_BATCH_MODE_MERGE_CONFIG = "bs_batch_mode_merge";
const std::string DEFAULT_ALTER_FIELD_MERGER_NAME = "AlterDefaultFieldMerger";
const std::string TASK_CONFIG_SUFFIX = "_task.json";

const std::string BS_TABLE_META_SYNCHRONIZER = "table_meta_synchronizer";
const std::string BS_TASK_DOC_RECLAIM = "doc_reclaim_task";
const std::string BS_TASK_END_BUILD = "end_build_task";
const std::string BS_TASK_GENERAL = "general_task";
const std::string BS_TASK_CLONE_INDEX = "clone_index_task";
const std::string BS_TASK_SYNC_INDEX = "sync_index_task";
const std::string BS_REPARTITION = "repartition";
const std::string BS_TASK_RUN_SCRIPT = "run_script";
const std::string BS_SNAPSHOT_VERSION = "snapshot_version";
const std::string BS_DROP_BUILDING_INDEX = "drop_building_index";
const std::string BS_BUILDER_TASK_V2 = "builderV2";

const std::string BS_ENDBUILD_VERSION = "endbuild_version";
const std::string BS_ENDBUILD_PARTITION_COUNT = "endbuild_partition_count";
const std::string BS_ENDBUILD_WORKER_PATHVERSION = "endbuild_worker_pathversion";

const std::string BS_BUILD_TASK_TARGET = "build_task_target";

const std::string BS_GENERAL_TASK = "general_task";
const std::string BS_TASK_PARALLEL_NUM = "parallel_num";
const std::string BS_TASK_THREAD_NUM = "thread_num";
const std::string BS_GENERAL_TASK_OPERATION_TARGET = "general_task_operation_target";
const std::string BS_GENERAL_TASK_EPOCH_ID = "general_task_epoch_id";
const std::string BS_GENERAL_TASK_SOURCE_VERSIONS = "general_task_source_versions";
const std::string BS_GENERAL_TASK_SOURCE_ROOT = "general_task_source_root";
const std::string BS_GENERAL_TASK_PARTITION_INDEX_ROOT = "general_task_partition_index_root";

const std::string BS_ROLLBACK_SOURCE_VERSION = "rollback_source_version";
const std::string BS_ROLLBACK_SOURCE_CHECKPOINT = "rollback_source_checkpoint";
const std::string BS_ROLLBACK_TARGET_VERSION = "rollback_target_version";

const std::string BS_TASK_BATCH_CONTROL = "batch_control_task";
const std::string BS_TASK_BATCH_CONTROL_ADDRESS = "admin_address";
const std::string BS_TASK_BATCH_CONTROL_PORT = "port";
const std::string BS_TOPIC_FULL = "topic_full";
const std::string BS_TOPIC_INC = "topic_inc";
const std::string BS_TASK_BATCH_CONTROL_TOPIC = "topic_name";
const std::string BS_TASK_BATCH_CONTROL_SWIFT_ROOT = "swift_root";
const std::string BS_TASK_BATCH_CONTROL_START_TS = "start_ts";
// builtin task

const std::string BS_EXTRACT_DOC = "extract_doc";
const std::string BS_EXTRACT_DOC_CHECKPOINT = "extract_doc_checkpoint";
const std::string BS_SYNC_INDEX_ROOT = "sync_index_root";
const std::string BS_PREPARE_DATA_SOURCE = "prepare_data_source";
const std::string BS_ROLLBACK_INDEX = "rollback_index";
const std::string BS_ENDBUILD = "endbuild";
const std::string BS_BUILDIN_BUILD_SERVICE_TASK_PREFIX = "libbuild_service_task.so";
const std::string BS_DOC_RECLAIM_CONF_FILE = "reclaim_config";

}} // namespace build_service::config
