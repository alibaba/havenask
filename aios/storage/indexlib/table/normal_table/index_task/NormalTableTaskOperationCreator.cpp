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
#include "indexlib/table/normal_table/index_task/NormalTableTaskOperationCreator.h"

#include "indexlib/framework/index_task/IndexOperation.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/field_meta/Common.h"
#include "indexlib/index/field_meta/config/FieldMetaConfig.h"
#include "indexlib/index/source/Common.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/add_index/PatchSegmentMoveOperation.h"
#include "indexlib/table/index_task/merger/CommonTaskOperationCreator.h"
#include "indexlib/table/normal_table/index_task/AttributeIndexMergeOperation.h"
#include "indexlib/table/normal_table/index_task/BucketMapOperation.h"
#include "indexlib/table/normal_table/index_task/Common.h"
#include "indexlib/table/normal_table/index_task/DeletionMapIndexMergeOperation.h"
#include "indexlib/table/normal_table/index_task/DropOpLogOperation.h"
#include "indexlib/table/normal_table/index_task/InvertedIndexMergeOperation.h"
#include "indexlib/table/normal_table/index_task/MultiShardInvertedIndexMergeOperation.h"
#include "indexlib/table/normal_table/index_task/NormalTableAddIndexOperation.h"
#include "indexlib/table/normal_table/index_task/NormalTableReclaimOperation.h"
#include "indexlib/table/normal_table/index_task/OpLog2PatchOperation.h"
#include "indexlib/table/normal_table/index_task/PackAttributeIndexMergeOperation.h"
#include "indexlib/table/normal_table/index_task/PrepareIndexReclaimParamOperation.h"
#include "indexlib/table/normal_table/index_task/ReclaimMapOperation.h"
#include "indexlib/table/normal_table/index_task/SourceIndexMergeOperation.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTableTaskOperationCreator);

NormalTableTaskOperationCreator::NormalTableTaskOperationCreator(const std::shared_ptr<config::ITabletSchema>& schema)
    : _schema(schema)
{
}

NormalTableTaskOperationCreator::~NormalTableTaskOperationCreator() {}

std::unique_ptr<framework::IndexOperation>
NormalTableTaskOperationCreator::CreateOperationForMerge(const framework::IndexOperationDescription& opDesc)
{
    const auto& typeStr = opDesc.GetType();
    if (typeStr == OpLog2PatchOperation::OPERATION_TYPE) {
        return std::make_unique<OpLog2PatchOperation>(opDesc);
    }
    if (typeStr == ReclaimMapOperation::OPERATION_TYPE) {
        return std::make_unique<ReclaimMapOperation>(opDesc);
    }
    if (typeStr == BucketMapOperation::OPERATION_TYPE) {
        return std::make_unique<BucketMapOperation>(opDesc);
    }
    if (typeStr == IndexMergeOperation::OPERATION_TYPE) {
        std::string indexName;
        std::string indexType;
        if (!opDesc.GetParameter(MERGE_INDEX_NAME, indexName) or !opDesc.GetParameter(MERGE_INDEX_TYPE, indexType)) {
            return nullptr;
        }
        std::unique_ptr<IndexMergeOperation> operation;
        if (indexType == index::ATTRIBUTE_INDEX_TYPE_STR) {
            auto attributeConfig = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(
                _schema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, indexName));
            if (attributeConfig == nullptr) {
                AUTIL_LOG(ERROR, "Invalid attribute config encountered[%s]:[%s]", indexType.c_str(), indexName.c_str());
                return nullptr;
            }
            operation.reset(new AttributeIndexMergeOperation(opDesc));
        } else if (indexType == index::PACK_ATTRIBUTE_INDEX_TYPE_STR) {
            operation = std::make_unique<PackAttributeIndexMergeOperation>(opDesc);
        } else if (indexType == index::DELETION_MAP_INDEX_TYPE_STR) {
            operation.reset(new DeletionMapIndexMergeOperation(opDesc));
        } else if (indexType == index::SOURCE_INDEX_TYPE_STR) {
            operation.reset(new SourceIndexMergeOperation(opDesc));
        } else if (indexType == indexlib::index::INVERTED_INDEX_TYPE_STR) {
            std::string shardIndexName;
            if (opDesc.GetParameter(SHARD_INDEX_NAME, shardIndexName)) {
                operation = std::make_unique<MultiShardInvertedIndexMergeOperation>(opDesc);
            } else {
                operation = std::make_unique<indexlib::table::InvertedIndexMergeOperation>(opDesc);
            }
        } else {
            operation = std::make_unique<IndexMergeOperation>(opDesc);
        }

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
NormalTableTaskOperationCreator::CreateOtherOperation(const framework::IndexOperationDescription& opDesc)
{
    AUTIL_LOG(ERROR, "can not create operation for [%s]", opDesc.GetType().c_str());
    return nullptr;
}

std::unique_ptr<framework::IndexOperation>
NormalTableTaskOperationCreator::CreateOperationForAlter(const framework::IndexOperationDescription& opDesc)
{
    if (opDesc.GetType() == NormalTableAddIndexOperation::OPERATION_TYPE) {
        auto operation = std::make_unique<NormalTableAddIndexOperation>(opDesc);
        return operation;
    }
    if (opDesc.GetType() == PatchSegmentMoveOperation::OPERATION_TYPE) {
        return std::make_unique<PatchSegmentMoveOperation>(opDesc);
    }

    if (opDesc.GetType() == DropOpLogOperation::OPERATION_TYPE) {
        return std::make_unique<DropOpLogOperation>(opDesc);
    }
    return nullptr;
}

std::unique_ptr<framework::IndexOperation>
NormalTableTaskOperationCreator::CreateOperationForReclaim(const framework::IndexOperationDescription& opDesc)
{
    if (opDesc.GetType() == NormalTableReclaimOperation::OPERATION_TYPE) {
        return std::make_unique<NormalTableReclaimOperation>(opDesc);
    }
    if (opDesc.GetType() == PrepareIndexReclaimParamOperation::OPERATION_TYPE) {
        return std::make_unique<PrepareIndexReclaimParamOperation>(opDesc);
    }
    return nullptr;
}

std::unique_ptr<framework::IndexOperation>
NormalTableTaskOperationCreator::CreateOperationForCommon(const framework::IndexOperationDescription& opDesc)
{
    CommonTaskOperationCreator commonCreator;
    return commonCreator.CreateOperation(opDesc);
}

std::unique_ptr<framework::IndexOperation>
NormalTableTaskOperationCreator::CreateOperation(const framework::IndexOperationDescription& opDesc)
{
    for (auto f : {&NormalTableTaskOperationCreator::CreateOperationForCommon,
                   &NormalTableTaskOperationCreator::CreateOperationForAlter,
                   &NormalTableTaskOperationCreator::CreateOperationForMerge,
                   &NormalTableTaskOperationCreator::CreateOperationForReclaim}) {
        auto operation = (this->*f)(opDesc);
        if (operation) {
            return operation;
        }
    }
    return CreateOtherOperation(opDesc);
}

} // namespace indexlibv2::table
