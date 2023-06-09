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
#include "indexlib/config/merge_config_base.h"

#include "indexlib/config/truncate_option_config.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, MergeConfigBase);

const string MergeConfigBase::DEFAULT_MERGE_STRATEGY = OPTIMIZE_MERGE_STRATEGY_STR;

MergeConfigBase::MergeConfigBase()
    : mergeStrategyStr(DEFAULT_MERGE_STRATEGY)
    , maxMemUseForMerge(DEFAULT_MAX_MEMORY_USE_FOR_MERGE)
    , mergeThreadCount(DEFAULT_MERGE_THREAD_COUNT)
    , uniqPackAttrMergeBufferSize(DEFAULT_UNIQ_ENCODE_PACK_ATTR_MERGE_BUFFER_SIZE)
    , enableMergeItemCheckPoint(true)
    , enableReclaimExpiredDoc(true)
{
}

MergeConfigBase::~MergeConfigBase() {}

void MergeConfigBase::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("merge_strategy", mergeStrategyStr, mergeStrategyStr);
    json.Jsonize("merge_strategy_params", mergeStrategyParameter, mergeStrategyParameter);
    json.Jsonize("document_reclaim_config_path", docReclaimConfigPath, docReclaimConfigPath);
    json.Jsonize("max_merge_memory_use", maxMemUseForMerge, maxMemUseForMerge);
    json.Jsonize("merge_thread_count", mergeThreadCount, mergeThreadCount);
    json.Jsonize("uniq_pack_attribute_merge_buffer_size", uniqPackAttrMergeBufferSize, uniqPackAttrMergeBufferSize);
    json.Jsonize("merge_io_config", mergeIOConfig, mergeIOConfig);
    json.Jsonize("enable_merge_item_check_point", enableMergeItemCheckPoint, enableMergeItemCheckPoint);
    json.Jsonize("enable_reclaim_expired_doc", enableReclaimExpiredDoc, enableReclaimExpiredDoc);

    if (json.GetMode() == FROM_JSON) {
        // support legacy format
        JsonMap jsonMap = json.GetMap();
        JsonMap::iterator iter = jsonMap.find("merge_strategy_params");
        if (iter == jsonMap.end()) {
            string legacyStr;
            json.Jsonize("merge_strategy_param", legacyStr, legacyStr);
            mergeStrategyParameter.SetLegacyString(legacyStr);
        }

        vector<TruncateStrategy> truncateStrategyVec;
        json.Jsonize("truncate_strategy", truncateStrategyVec, truncateStrategyVec);
        if (!truncateStrategyVec.empty()) {
            truncateOptionConfig.reset(new TruncateOptionConfig(truncateStrategyVec));
            truncateOptionConfig->Check(IndexPartitionSchemaPtr());
        }
    }
}

void MergeConfigBase::Check() const
{
    if (maxMemUseForMerge == 0) {
        INDEXLIB_FATAL_ERROR(BadParameter, "max_merge_memory_use should be greater than 0.");
    }

    if (uniqPackAttrMergeBufferSize == 0) {
        INDEXLIB_FATAL_ERROR(BadParameter, "uniq_pack_attribute_merge_buffer_size"
                                           " should be greater than 0.");
    }

    if (mergeThreadCount == 0 || mergeThreadCount > MAX_MERGE_THREAD_COUNT) {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "merge_thread_count should be greater than 0"
                             " and no more than %d.",
                             MAX_MERGE_THREAD_COUNT);
    }

    if (truncateOptionConfig) {
        truncateOptionConfig->Check(IndexPartitionSchemaPtr());
    }
}
}} // namespace indexlib::config
