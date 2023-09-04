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

#include "indexlib/base/Status.h"
#include "indexlib/framework/index_task/IndexOperationDescription.h"
#include "indexlib/index/common/patch/PatchFileInfos.h"
#include "indexlib/table/index_task/merger/IndexMergeOperation.h"

namespace indexlibv2::table {
class AttributeIndexMergeOperation : public IndexMergeOperation
{
public:
    AttributeIndexMergeOperation(const framework::IndexOperationDescription& opDesc);
    ~AttributeIndexMergeOperation() {}

    Status Execute(const framework::IndexTaskContext& context) override;

private:
    std::pair<Status, std::shared_ptr<config::IIndexConfig>>
    GetIndexConfig(const framework::IndexOperationDescription& opDesc, const std::string& indexType,
                   const std::string& indexName, const std::shared_ptr<config::ITabletSchema>& schema) override;

private:
    indexlibv2::index::PatchInfos _allPatchInfos;

private:
    friend class AttributeIndexMergeOperationTest;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
