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
#include "indexlib/table/normal_table/index_task/SourceIndexMergeOperation.h"

#include "indexlib/base/PathUtil.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/source/Common.h"
#include "indexlib/index/source/config/SourceIndexConfig.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/normal_table/index_task/Common.h"

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, SourceIndexMergeOperation);

SourceIndexMergeOperation::SourceIndexMergeOperation(const framework::IndexOperationDescription& opDesc)
    : IndexMergeOperation(opDesc)
{
}

std::pair<Status, std::shared_ptr<config::IIndexConfig>>
SourceIndexMergeOperation::GetIndexConfig(const framework::IndexOperationDescription& opDesc,
                                          const std::string& indexType, const std::string& indexName,
                                          const std::shared_ptr<config::ITabletSchema>& schema)
{
    auto indexConfig = schema->GetIndexConfig(indexType, indexName);
    if (!indexConfig) {
        AUTIL_LOG(ERROR, "get index config failed. index type [%s], index name [%s]", indexType.c_str(),
                  indexName.c_str());
        assert(false);
        return std::make_pair(Status::Corruption("get index config failed"), nullptr);
    }

    auto sourceConfig = std::dynamic_pointer_cast<indexlibv2::config::SourceIndexConfig>(indexConfig);

    if (sourceConfig == nullptr) {
        auto status = Status::Corruption("Invalid source config encountered [%s]", indexName.c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return std::make_pair(status, nullptr);
    }
    int32_t sourceMergeId = 0;
    if (opDesc.GetParameter(index::SOURCE_MERGE_ID, sourceMergeId)) {
        auto sourceMergeConfigs = sourceConfig->CreateSourceIndexConfigForMerge();
        if (sourceMergeId > sourceMergeConfigs.size()) {
            AUTIL_LOG(ERROR, "source merge id is [%d], but merge config size is [%lu]", sourceMergeId,
                      sourceMergeConfigs.size());
            return {Status::Corruption("source merge id is invalid"), {}};
        }
        return std::make_pair(Status::OK(), sourceMergeConfigs[sourceMergeId]);
    }
    return std::make_pair(Status::Corruption(), nullptr);
}

}} // namespace indexlibv2::table
