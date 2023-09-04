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

#include "autil/Log.h"
#include "indexlib/framework/index_task/BasicDefs.h"
#include "indexlib/table/index_task/merger/CommonMergeDescriptionCreator.h"
#include "indexlib/table/normal_table/Common.h"

namespace indexlibv2::table {

class NormalTableMergeDescriptionCreator : public CommonMergeDescriptionCreator
{
public:
    NormalTableMergeDescriptionCreator(const std::shared_ptr<config::ITabletSchema>& schema,
                                       const std::string& mergeStrategy, const std::string& compactionType,
                                       bool isOptimizeMerge);
    ~NormalTableMergeDescriptionCreator();

public:
    static std::unique_ptr<framework::IndexOperationDescription>
    CreateOpLog2PatchOperationDescription(const std::shared_ptr<config::ITabletSchema>& schema,
                                          framework::IndexOperationId operationId);

protected:
    std::unique_ptr<framework::IndexOperationDescription>
    CreateInitMergeOperationDescription(const std::shared_ptr<MergePlan>& mergePlan,
                                        framework::IndexOperationId operationId) override;

    std::pair<framework::IndexOperationId, std::vector<std::unique_ptr<framework::IndexOperationDescription>>>
    CreateBeforeMergeIndexOperationDescriptions(framework::IndexOperationId operationId,
                                                framework::IndexOperationDescription* preOpDesc,
                                                size_t mergePlanIdx) override;

    std::pair<Status, framework::IndexOperationDescription>
    CreateIndexMergeOperationDescription(const std::shared_ptr<MergePlan>& mergePlan,
                                         const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                         framework::IndexOperationId operationId, size_t planIdx) override;

    std::vector<std::shared_ptr<config::IIndexConfig>> GetSupportConfigs() override;

private:
    bool _isSortedMerge = false;
    bool _isOptimizeMerge = false;
    framework::IndexOperationId _bucketMapOpId = framework::INVALID_INDEX_OPERATION_ID;
    std::string _compactionType = NORMAL_TABLE_MERGE_TYPE;
    std::string _mergeStrategy;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
