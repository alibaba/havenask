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
#include "indexlib/index/operation_log/OperationFactory.h"

#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/index/operation_log/OperationLogConfig.h"
#include "indexlib/index/operation_log/RemoveOperation.h"
#include "indexlib/index/operation_log/RemoveOperationCreator.h"
#include "indexlib/index/operation_log/UpdateFieldOperation.h"
#include "indexlib/index/operation_log/UpdateFieldOperationCreator.h"

namespace indexlib::index {
namespace {
} // namespace

AUTIL_LOG_SETUP(indexlib.index, OperationFactory);

OperationFactory::OperationFactory() : _mainPkType(it_primarykey64) {}

OperationFactory::~OperationFactory() {}

void OperationFactory::Init(const std::shared_ptr<OperationLogConfig>& config)
{
    assert(config->GetPrimaryKeyIndexConfig());
    _mainPkType = config->GetPrimaryKeyIndexConfig()->GetInvertedIndexType();
    assert(_mainPkType == it_primarykey64 || _mainPkType == it_primarykey128);
    _removeOperationCreator = CreateRemoveOperationCreator(config);
    _updateOperationCreator = CreateUpdateOperationCreator(config);
}

bool OperationFactory::CreateOperation(const indexlibv2::document::NormalDocument* doc, autil::mem_pool::Pool* pool,
                                       OperationBase** operation)
{
    DocOperateType opType = doc->GetDocOperateType();
    if (opType == ADD_DOC || opType == DELETE_DOC) {
        return _removeOperationCreator->Create(doc, pool, operation);
    }
    if (opType == UPDATE_FIELD) {
        return _updateOperationCreator->Create(doc, pool, operation);
    }
    // This happens e.g. when the entire update doc is skipped because there is no valid attribute/inverted index to
    // update.
    if (opType == SKIP_DOC) {
        return true;
    }
    assert(false);
    return false;
}

std::shared_ptr<OperationCreator>
OperationFactory::CreateUpdateOperationCreator(const std::shared_ptr<OperationLogConfig>& config)
{
    return std::shared_ptr<OperationCreator>(new UpdateFieldOperationCreator(config));
}

std::shared_ptr<OperationCreator>
OperationFactory::CreateRemoveOperationCreator(const std::shared_ptr<OperationLogConfig>& config)
{
    return std::shared_ptr<OperationCreator>(new RemoveOperationCreator(config));
}

OperationBase* OperationFactory::CreateUnInitializedOperation(OperationBase::SerializedOperationType opType,
                                                              int64_t timestamp, uint16_t hashId,
                                                              autil::mem_pool::Pool* pool) const
{
    OperationBase* operation = NULL;

    switch (opType) {
    case OperationBase::REMOVE_OP:
        operation = (_mainPkType == it_primarykey64)
                        ? static_cast<OperationBase*>(
                              IE_POOL_COMPATIBLE_NEW_CLASS(pool, RemoveOperation<uint64_t>, timestamp, hashId))
                        : static_cast<OperationBase*>(
                              IE_POOL_COMPATIBLE_NEW_CLASS(pool, RemoveOperation<autil::uint128_t>, timestamp, hashId));
        break;
    case OperationBase::UPDATE_FIELD_OP:
        operation = (_mainPkType == it_primarykey64)
                        ? static_cast<OperationBase*>(
                              IE_POOL_COMPATIBLE_NEW_CLASS(pool, UpdateFieldOperation<uint64_t>, timestamp, hashId))
                        : static_cast<OperationBase*>(IE_POOL_COMPATIBLE_NEW_CLASS(
                              pool, UpdateFieldOperation<autil::uint128_t>, timestamp, hashId));
        break;
    default:
        assert(false);
    }
    return operation;
}

std::pair<Status, OperationBase*>
OperationFactory::DeserializeOperation(const char* buffer, autil::mem_pool::Pool* pool, size_t& opSize) const
{
    assert(buffer);
    char* baseAddr = const_cast<char*>(buffer);
    uint8_t serializeOpType = *(uint8_t*)baseAddr;
    OperationBase::SerializedOperationType opType = (OperationBase::SerializedOperationType)serializeOpType;
    baseAddr += sizeof(serializeOpType);
    int64_t timestamp = *(int64_t*)baseAddr;
    baseAddr += sizeof(timestamp);
    uint16_t hashId = *(uint16_t*)baseAddr;
    baseAddr += sizeof(hashId);
    OperationBase* operation = CreateUnInitializedOperation(opType, timestamp, hashId, pool);
    if (!operation) {
        AUTIL_LOG(ERROR, "deserialize operation type failed");
        return std::make_pair(
            Status::Corruption("Deserialize operation type:", serializeOpType, " failed.", serializeOpType), nullptr);
    }
    operation->Load(pool, baseAddr);
    opSize = (const char*)baseAddr - buffer;
    return std::make_pair(Status::OK(), operation);
}

} // namespace indexlib::index
