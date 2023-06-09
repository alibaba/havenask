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
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/index_task/IndexOperationDescription.h"
#include "indexlib/table/index_task/merger/MergePlan.h"
#include "indexlib/table/index_task/merger/MergedVersionCommitOperation.h"

namespace indexlibv2::table {

class CommonMergeDescriptionCreator
{
public:
    CommonMergeDescriptionCreator(const std::shared_ptr<config::TabletSchema>& schema);
    ~CommonMergeDescriptionCreator();

public:
    std::pair<Status, std::vector<framework::IndexOperationDescription>>
    CreateMergeOperationDescriptions(const std::shared_ptr<MergePlan>& mergePlan);

protected:
    virtual std::unique_ptr<framework::IndexOperationDescription>
    CreateInitMergeOperationDescription(const std::shared_ptr<MergePlan>& mergePlan,
                                        framework::IndexOperationId operationId)
    {
        return nullptr;
    }
    virtual std::pair<framework::IndexOperationId, std::vector<std::unique_ptr<framework::IndexOperationDescription>>>
    CreateBeforeMergeIndexOperationDescriptions(framework::IndexOperationId operationId,
                                                framework::IndexOperationDescription* preOpDesc, size_t mergePlanIdx)
    {
        return {framework::INVALID_INDEX_OPERATION_ID,
                std::vector<std::unique_ptr<framework::IndexOperationDescription>>()};
    }

    virtual std::pair<Status, framework::IndexOperationDescription>
    CreateIndexMergeOperationDescription(const std::shared_ptr<MergePlan>& mergePlan,
                                         const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                         framework::IndexOperationId operationId, size_t planIdx);
    virtual std::vector<std::shared_ptr<config::IIndexConfig>> GetSupportConfigs();

protected:
    std::shared_ptr<config::TabletSchema> _schema;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
