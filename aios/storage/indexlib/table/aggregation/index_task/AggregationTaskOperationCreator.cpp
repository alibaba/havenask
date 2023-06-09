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
#include "indexlib/table/aggregation/index_task/AggregationTaskOperationCreator.h"

#include "indexlib/table/index_task/merger/CommonTaskOperationCreator.h"
#include "indexlib/table/index_task/merger/IndexMergeOperation.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, AggregationTaskOperationCreator);

AggregationTaskOperationCreator::AggregationTaskOperationCreator(const std::shared_ptr<config::TabletSchema>& schema)
    : _schema(schema)
{
}

AggregationTaskOperationCreator::~AggregationTaskOperationCreator() = default;

std::unique_ptr<framework::IndexOperation>
AggregationTaskOperationCreator::CreateOperation(const framework::IndexOperationDescription& opDesc)
{
    if (opDesc.GetType() == IndexMergeOperation::OPERATION_TYPE) {
        auto operation = std::make_unique<IndexMergeOperation>(opDesc);
        auto s = operation->Init(_schema);
        if (!s.IsOK()) {
            AUTIL_LOG(ERROR, "init index merge operation failed [%s]", s.ToString().c_str());
            return nullptr;
        }
        return operation;
    } else {
        CommonTaskOperationCreator commonCreator;
        return commonCreator.CreateOperation(opDesc);
    }
}

} // namespace indexlibv2::table
