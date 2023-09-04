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
#include "indexlib/table/kkv_table/index_task/KKVTableTaskOperationCreator.h"

#include "indexlib/table/index_task/merger/CommonTaskOperationCreator.h"
#include "indexlib/table/kkv_table/index_task/KKVIndexMergeOperation.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KKVTableTaskOperationCreator);

KKVTableTaskOperationCreator::KKVTableTaskOperationCreator(const std::shared_ptr<config::ITabletSchema>& schema)
    : _schema(schema)
{
}

KKVTableTaskOperationCreator::~KKVTableTaskOperationCreator() {}

std::unique_ptr<framework::IndexOperation>
KKVTableTaskOperationCreator::CreateOperationForCommon(const framework::IndexOperationDescription& opDesc)
{
    CommonTaskOperationCreator commonCreator;
    return commonCreator.CreateOperation(opDesc);
}

std::unique_ptr<framework::IndexOperation>
KKVTableTaskOperationCreator::CreateOperationForMerge(const framework::IndexOperationDescription& opDesc)
{
    if (opDesc.GetType() == IndexMergeOperation::OPERATION_TYPE) {
        auto operation = std::make_unique<KKVIndexMergeOperation>(opDesc);
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
KKVTableTaskOperationCreator::CreateOperation(const framework::IndexOperationDescription& opDesc)
{
    for (auto f : {&KKVTableTaskOperationCreator::CreateOperationForCommon,
                   &KKVTableTaskOperationCreator::CreateOperationForMerge}) {
        auto operation = (this->*f)(opDesc);
        if (operation) {
            return operation;
        }
    }
    assert(false);
    return nullptr;
}

} // namespace indexlibv2::table
