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

#include "indexlib/framework/index_task/Constant.h"

namespace indexlibv2::table {

static constexpr const char* MERGE_INDEX_TYPE = "merge_index_type";
static constexpr const char* MERGE_INDEX_NAME = "merge_index_name";
static constexpr const char* MERGE_PLAN_INDEX = "merge_plan_index";
static constexpr const char* MERGE_PLAN = "merge_plan";
static constexpr const char* SST_TO_SEGMENT_PLAN = "sst_to_segment_plan";
static constexpr const char* VERSION_RESOURCE = "version_resource";
static constexpr const char* IS_OPTIMIZE_MERGE = "is_optimize_merge";
static constexpr const char* NEED_CLEAN_OLD_VERSIONS = "need_clean_old_versions";
static constexpr const char* RESERVED_VERSION_SET = "reserved_version_set";
static constexpr const char* RESERVED_VERSION_COORD_SET = "reserved_version_coord_set";
static constexpr const char* PARAM_TARGET_VERSION = "target_version";
static constexpr const char* PARAM_TARGET_VERSION_ID = "target_version_id";
static constexpr const char* SEGMENT_METRICS_TMP_PATH = "segment_metrics_tmp_path";
static constexpr const char* DEPENDENT_OPERATION_ID = "dependent_operation_id";
static constexpr const char* SHARD_INDEX_NAME = "multi_shard_inverted_index";
static constexpr const char* TASK_NAME = "task_name";
static constexpr const char* DESIGNATE_FULL_MERGE_TASK_NAME = "full_merge";
static constexpr const char* DESIGNATE_BATCH_MODE_MERGE_TASK_NAME = "bs_batch_mode_merge";
static constexpr const char* BRANCH_ID = "_branch_id_";

static constexpr const char* MERGE_TASK_TYPE = "merge";
static constexpr const char* RECLAIM_TASK_TYPE = "reclaim";

} // namespace indexlibv2::table
