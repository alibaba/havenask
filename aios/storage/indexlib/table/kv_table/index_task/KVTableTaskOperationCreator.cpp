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
#include "indexlib/table/kv_table/index_task/KVTableTaskOperationCreator.h"

#include <memory>

#include "indexlib/table/index_task/merger/CommonTaskOperationCreator.h"
#include "indexlib/table/kv_table/index_task/KVIndexMergeOperation.h"
#include "indexlib/table/kv_table/index_task/KVTableBulkloadOperation.h"
#include "indexlib/table/kv_table/index_task/KVTableBulkloadSegmentCreateOperation.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KVTableTaskOperationCreator);

KVTableTaskOperationCreator::KVTableTaskOperationCreator(const std::shared_ptr<config::ITabletSchema>& schema)
    : _schema(schema)
{
}

KVTableTaskOperationCreator::~KVTableTaskOperationCreator() {}

std::unique_ptr<framework::IndexOperation>
KVTableTaskOperationCreator::CreateOperationForCommon(const framework::IndexOperationDescription& opDesc)
{
    CommonTaskOperationCreator commonCreator;
    return commonCreator.CreateOperation(opDesc);
}

std::unique_ptr<framework::IndexOperation>
KVTableTaskOperationCreator::CreateOperationForMerge(const framework::IndexOperationDescription& opDesc)
{
    if (opDesc.GetType() == IndexMergeOperation::OPERATION_TYPE) {
        auto operation = std::make_unique<KVIndexMergeOperation>(opDesc);
        auto status = operation->Init(_schema);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "init index merge operation failed [%s]", status.ToString().c_str());
            return nullptr;
        }
        return operation;
    }
    return nullptr;
}

std::unique_ptr<framework::IndexOperation>
KVTableTaskOperationCreator::CreateOperationForBulkload(const framework::IndexOperationDescription& opDesc)
{
    if (opDesc.GetType() == KVTableBulkloadOperation::OPERATION_TYPE) {
        return std::make_unique<KVTableBulkloadOperation>(opDesc);
    } else if (opDesc.GetType() == KVTableBulkloadSegmentCreateOperation::OPERATION_TYPE) {
        return std::make_unique<KVTableBulkloadSegmentCreateOperation>(opDesc);
    }
    return nullptr;
}

std::unique_ptr<framework::IndexOperation>
KVTableTaskOperationCreator::CreateOperation(const framework::IndexOperationDescription& opDesc)
{
    for (auto f :
         {&KVTableTaskOperationCreator::CreateOperationForCommon, &KVTableTaskOperationCreator::CreateOperationForMerge,
          &KVTableTaskOperationCreator::CreateOperationForBulkload}) {
        auto operation = (this->*f)(opDesc);
        if (operation) {
            return operation;
        }
    }
    assert(false);
    return nullptr;
}

} // namespace indexlibv2::table
