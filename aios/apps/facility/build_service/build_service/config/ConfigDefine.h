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
#pragma once

#include <string>

namespace build_service { namespace config {

extern const std::string SCHEMA_CONFIG_PATH;
extern const std::string SCHEMA_JSON_FILE_SUFFIX;
extern const std::string SCHEMA_PATCH_JSON_FILE_SUFFIX;
extern const std::string CLUSTER_CONFIG_PATH;
extern const std::string PROCESSOR_CONFIG_PATH;
extern const std::string CLUSTER_JSON_FILE_SUFFIX;
extern const std::string LUA_CONFIG_PATH;
extern const std::string GRAPH_CONFIG_FILE;
extern const std::string AGENT_GROUP_CONFIG_FILE;
extern const std::string RAW_DOC_FUNCTION_RESOURCE_FILE;
extern const std::string DATA_TABLE_PATH;
extern const std::string DATA_TABLE_JSON_FILE_SUFFIX;
extern const std::string TASK_SCRIPTS_CONFIG_PATH;

extern const std::string BUILD_APP_FILE_NAME;
extern const std::string HIPPO_FILE_NAME;
constexpr char MODIFIED_FIELDS_DOCUMENT_PROCESSOR_NAME[] = "ModifiedFieldsDocumentProcessor";

extern const std::string INHERIT_FROM_KEY;
extern const std::string INHERIT_FROM_EXTERNAL_KEY;

extern const std::string BS_FIELD_SEPARATOR;
extern const std::string BS_FILE_NAME;
extern const std::string BS_LINE_DATA;
extern const std::string BS_ALTER_FIELD_MERGE_CONFIG;
extern const std::string BS_BATCH_MODE_MERGE_CONFIG;
extern const std::string DEFAULT_ALTER_FIELD_MERGER_NAME;
extern const std::string TASK_CONFIG_SUFFIX;
constexpr int64_t INVALID_SCHEMAVERSION = -1;

extern const std::string BS_TABLE_META_SYNCHRONIZER;
extern const std::string BS_TASK_DOC_RECLAIM;
extern const std::string BS_TASK_END_BUILD;
extern const std::string BS_TASK_GENERAL;
extern const std::string BS_TASK_CLONE_INDEX;
extern const std::string BS_TASK_SYNC_INDEX;
extern const std::string BS_REPARTITION;
extern const std::string BS_TASK_RUN_SCRIPT;
extern const std::string BS_SNAPSHOT_VERSION;
extern const std::string BS_DROP_BUILDING_INDEX;
extern const std::string BS_BUILDER_TASK_V2;

extern const std::string BS_ENDBUILD_VERSION;
extern const std::string BS_ENDBUILD_PARTITION_COUNT;
extern const std::string BS_ENDBUILD_WORKER_PATHVERSION;

extern const std::string BS_BUILD_TASK_TARGET;
extern const std::string BS_APP_NAME;
extern const std::string BS_GENERAL_TASK;
extern const std::string BS_TASK_PARALLEL_NUM;
extern const std::string BS_TASK_THREAD_NUM;
extern const std::string BS_GENERAL_TASK_OPERATION_TARGET;
extern const std::string BS_GENERAL_TASK_EPOCH_ID;
extern const std::string BS_GENERAL_TASK_SOURCE_VERSIONS;
extern const std::string BS_GENERAL_TASK_SOURCE_ROOT;
extern const std::string BS_GENERAL_TASK_PARTITION_INDEX_ROOT;

extern const std::string BS_ROLLBACK_SOURCE_VERSION;
extern const std::string BS_ROLLBACK_SOURCE_CHECKPOINT;
extern const std::string BS_ROLLBACK_TARGET_VERSION;

extern const std::string BS_TASK_BATCH_CONTROL;
extern const std::string BS_TASK_BATCH_CONTROL_ADDRESS;
extern const std::string BS_TASK_BATCH_CONTROL_PORT;
extern const std::string BS_TOPIC_FULL;
extern const std::string BS_TOPIC_INC;
extern const std::string BS_TASK_BATCH_CONTROL_TOPIC;
extern const std::string BS_TASK_BATCH_CONTROL_SWIFT_ROOT;
extern const std::string BS_TASK_BATCH_CONTROL_START_TS;
// builtin task

extern const std::string BS_EXTRACT_DOC;
extern const std::string BS_EXTRACT_DOC_CHECKPOINT;
extern const std::string BS_SYNC_INDEX_ROOT;
extern const std::string BS_PREPARE_DATA_SOURCE;
extern const std::string BS_ROLLBACK_INDEX;
extern const std::string BS_ENDBUILD;
extern const std::string BS_BUILDIN_BUILD_SERVICE_TASK_PREFIX;
extern const std::string BS_DOC_RECLAIM_CONF_FILE;

}} // namespace build_service::config
