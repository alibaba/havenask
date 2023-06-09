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
#include "build_service/common_define.h"

namespace build_service {

const std::string EMPTY_STRING = "";
const std::string BS_COUNTER_PREFIX = "bs";

const std::string BUILDER_EPOCH_ID = "epoch_id";
const std::string BUILDER_BRANCH_NAME_LEGACY = "branch_name_legacy";

const std::string REALTIME_INFO_FILE_NAME = "realtime_info.json";
const std::string COUNTER_DIR_NAME = "counter";
const std::string NO_MERGER_CHECKPOINT_FILE_NAME = "no_merger_checkpoint";
const std::string SCHEMA_PATCH = "schema_patch";
// bs_admin_env
const std::string BS_ENV_RECOVER_THREAD_NUM = "recover_thread_num";
const std::string BS_ENV_RECOVER_SLEEP_SECOND = "bs_recover_sleep_second";
const std::string BS_ENV_FILL_COUNTER_INFO = "fill_counter_info";
const std::string BS_ENV_ENABLE_GENERATION_METRICS = "enable_generation_metrics";
const std::string BS_ENV_ADMIN_SWIFT_TIMEOUT = "bs_admin_swift_timeout";
const std::string BS_ENV_HEALTH_CHECK_SERVICEKEEPER_TIMEOUT = "bs_health_check_servicekeeper_timeout";       // second
const std::string BS_ENV_HEALTH_CHECK_GENERATIONKEEPER_TIMEOUT = "bs_health_check_generationkeeper_timeout"; // second
const std::string BS_ENV_SYNC_COUNTER_INTERVAL = "bs_sync_counter_interval";                                 // second
const std::string BS_ENV_MEM_PRINTING_INTERVAL = "bs_tcmalloc_mem_printing_interval";                        // second
const std::string BS_ENV_ENABLE_LEGACY_BUILDER_METRICS = "bs_enable_legacy_builder_metrics";
const std::string BS_ENV_MEM_RELEASE_INTERVAL = "bs_tcmalloc_mem_release_interval"; // second
// global resource reader cache
const std::string BS_ENV_ENABLE_RESOURCE_READER_CACHE = "bs_resource_reader_cache"; // second
const std::string BS_ENV_ENABLE_RESOURCE_READER_CACHE_CLEAR_INTERVAL =
    "bs_resource_reader_cache_clear_interval";                                                              // second
const std::string BS_ENV_ENABLE_RESOURCE_READER_CACHE_EXPIRE_TIME = "bs_resource_reader_cache_expire_time"; // second
const std::string BS_ENV_ENABLE_RESOURCE_READER_MAX_ZIP_SIZE = "bs_resource_reader_max_zip_size";           // kilobyte
const std::string BS_ENV_TEMP_DIR = "bs_admin_temp_dir";
const std::string BS_ENV_FLOW_CLEAR_INTERVAL = "bs_admin_flow_clear_interval"; // second
const std::string BS_ENV_DISABLE_BATCH_BROKER_TOPIC = "disable_batch_broker_topic";
const std::string BS_ENV_BROKER_TOPIC_MAX_BATCH_SIZE = "broker_topic_max_batch_size";

// checkpoint
const std::string BS_RESOURCE_CHECKPOINT_ID = "checkpoint_resource_checkpoint";
const std::string BS_BUILDER_CHECKPOINT_ID = "builder_checkpoint";
const std::string BS_INDEX_INFO_ID = "index_info";
const std::string BS_INDEX_CHECKPOINT_ID = "index_checkpoint";
const std::string BS_INDEX_TOTAL_SIZE_ID = "index_total_size";
const std::string BS_BRANCH_INDEX_CHECKPOINT_ID = "branch_index_checkpoint";
const std::string BS_PROCESSOR_CHECKPOINT_ID = "processor_checkpoint";
const std::string BS_PROCESSOR_MAX_ID = "bs_processor_max_id";
const std::string BS_CKP_ID_BUILDER_PATH_VERSION_PREFIX = "builder_worker_path_version_";
const std::string BS_CKP_NAME_WORKER_PATH_VERSION = "worker_path_version";
const std::string BS_CKP_ID_DISABLE_OPIDS = "disable_opids";
const std::string BS_CKP_SCHEMA_PATCH = "schema_patch";

// flow related
const std::string BS_ENV_LUA_CONFIG_PATH = "control_flow_config_path";
const std::string BS_FULL_PROCESSOR_FLOW_ID = "fd_full_processor";
const std::string BS_INC_PROCESSOR_FLOW_ID = "fd_inc_processor";
const std::string BS_FULL_BUILD_FLOW_ID = "fd_full_build";
const std::string BS_INC_BUILD_FLOW_ID = "fd_inc_build";
const std::string BS_ALTER_FIELD_FLOW_ID = "fd_alter_field";
const std::string BS_MERGECRONTAB_FLOW_ID = "fd_merge_crontab";
const std::string BS_PREPARE_DATA_SOURCE_ID = "fd_prepare_data_source";
const std::string BS_PREPARE_VERSION_FLOW_ID = "fd_prepare_version";
const std::string BS_ROLLBACK_FLOW_ID = "fd_rollback";

const std::string BS_FLOW_ID_MERGECRONTAB_SUFFIX = "_merge_crontab";
const std::string BS_TASK_ID_MERGECRONTAB = "mergeCrontab";

const std::string BS_ENV_INDEX_INFO_REPORT_INTERVAL = "bs_index_info_report_interval";
const std::string BS_ENV_INDEX_INFO_CLEAR_INTERVAL = "bs_index_info_clear_interval";

const std::string BS_ENV_ENABLE_SCHEDULE_METRIC = "bs_enable_schedule_metric";
const std::string BS_ENV_ADMIN_KMON_SERVICE_NAME = "bs_admin_kmon_service_name";
const std::string BS_ENV_ADMIN_DEFAULT_KMON_SERVICE_NAME = "bs.admin";
const std::string BS_ENV_ADMIN_TASK_BEEPER_REPORT_INTERVAL = "bs_admin_task_beeper_report_interval";
const std::string BS_ENV_ADMIN_RESTART_THRESHOLD_TO_REPORT = "bs_admin_restart_threshold_report";

const std::string BS_ENV_KMON_SERVICE_NAME = "bs_kmon_service_name";
const std::string BS_ENV_DEFAULT_KMON_SERVICE_NAME = "bs.worker";

const std::string BS_ENV_DOC_TRACE_FIELD = "bs_doc_trace_field";
const std::string BS_ENV_DOC_TRACE_INTERVAL = "bs_doc_trace_interval"; // second
const std::string BS_ENV_READER_CHECKPOINT_STEP = "bs_doc_reader_checkpoint_step";

const std::string BS_BACKUP_NODE_LABEL = "backup";

}; // namespace build_service
