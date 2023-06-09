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
#include "indexlib/table/index_task/merger/CommonMergeDescriptionCreator.h"

namespace indexlibv2::table {

class KVTableMergeDescriptionCreator : public CommonMergeDescriptionCreator
{
public:
    KVTableMergeDescriptionCreator(const std::shared_ptr<config::TabletSchema>& schema);
    ~KVTableMergeDescriptionCreator();

protected:
    std::pair<Status, framework::IndexOperationDescription>
    CreateIndexMergeOperationDescription(const std::shared_ptr<MergePlan>& mergePlan,
                                         const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                         framework::IndexOperationId operationId, size_t planIdx) override;

private:
    int64_t _currentTimestamp = -1;
    friend class KVTableMergeDescriptionCreatorTest;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
