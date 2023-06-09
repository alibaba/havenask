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
#include "indexlib/table/normal_table/index_task/InvertedIndexMergeOperation.h"

namespace indexlibv2 {
namespace framework {
class IndexOperationDescription;
}
namespace config {
class TabletSchema;
}
} // namespace indexlibv2
namespace indexlibv2::table {

class MultiShardInvertedIndexMergeOperation : public indexlib::table::InvertedIndexMergeOperation
{
public:
    MultiShardInvertedIndexMergeOperation(const framework::IndexOperationDescription& opDesc);
    ~MultiShardInvertedIndexMergeOperation();

protected:
    std::pair<Status, std::shared_ptr<config::IIndexConfig>>
    GetIndexConfig(const framework::IndexOperationDescription& opDesc, const std::string& indexType,
                   const std::string& indexName, const std::shared_ptr<config::TabletSchema>& schema) override final;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
