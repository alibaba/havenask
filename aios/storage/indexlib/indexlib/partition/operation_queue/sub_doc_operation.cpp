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
#include "indexlib/partition/operation_queue/sub_doc_operation.h"

#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"
#include "indexlib/partition/modifier/sub_doc_modifier.h"
#include "indexlib/partition/operation_queue/remove_operation.h"
#include "indexlib/partition/operation_queue/update_field_operation.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::index;
using namespace indexlibv2::index;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, SubDocOperation);

SubDocOperation::~SubDocOperation()
{
    if (mMainOperation) {
        mMainOperation->~OperationBase();
    }
    for (size_t i = 0; i < mSubOperationCount; ++i) {
        mSubOperations[i]->~OperationBase();
    }
}

void SubDocOperation::Init(DocOperateType docOperateType, OperationBase* mainOperation, OperationBase** subOperations,
                           size_t subOperationCount)
{
    mDocOperateType = docOperateType;
    mMainOperation = mainOperation;
    mSubOperations = subOperations;
    mSubOperationCount = subOperationCount;
}

bool SubDocOperation::Process(const PartitionModifierPtr& modifier, const OperationRedoHint& redoHint,
                              future_lite::Executor* executor)
{
    SubDocModifierPtr subDocModifier = DYNAMIC_POINTER_CAST(SubDocModifier, modifier);
    assert(subDocModifier);
    if (!subDocModifier) {
        IE_LOG(ERROR, "cast modifier to SubDocModifier failed");
        return false;
    }

    if (mMainOperation) {
        if (!mMainOperation->Process(subDocModifier->GetMainModifier(), redoHint, executor)) {
            return false;
        }
    }
    if (mSubOperationCount > 0) {
        const PartitionModifierPtr& subModifier = subDocModifier->GetSubModifier();
        OperationRedoHint subRedoHint;
        if (redoHint.GetHintType() == OperationRedoHint::REDO_CACHE_SEGMENT) {
            subRedoHint.SetHintType(redoHint.GetHintType());
            const auto& segmentIds = redoHint.GetCachedSegmentIds();
            for (size_t i = 0; i < segmentIds.size(); i++) {
                subRedoHint.AddCachedSegment(segmentIds[i]);
            }
        } else if (redoHint.GetHintType() == OperationRedoHint::REDO_DOC_RANGE) {
            subRedoHint.SetHintType(redoHint.GetHintType());
            subRedoHint.SetCachedDocRanges(redoHint.GetCachedDocRange().second, {});
        }
        for (size_t i = 0; i < mSubOperationCount; ++i) {
            assert(mSubOperations[i]);
            if (!mSubOperations[i]->Process(subModifier, subRedoHint, executor)) {
                return false;
            }
        }
    }
    return true;
}

OperationBase* SubDocOperation::Clone(Pool* pool)
{
    OperationBase* mainOperation = NULL;
    if (mMainOperation) {
        mainOperation = mMainOperation->Clone(pool);
    }

    OperationBase** subOperations = NULL;
    if (mSubOperationCount > 0) {
        assert(mSubOperations);
        subOperations = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, OperationBase*, mSubOperationCount);
        if (!subOperations) {
            IE_LOG(ERROR, "allocate memory fail!");
            return NULL;
        }
        for (size_t i = 0; i < mSubOperationCount; ++i) {
            assert(mSubOperations[i]);
            subOperations[i] = mSubOperations[i]->Clone(pool);
        }
    }

    SubDocOperation* operation =
        IE_POOL_COMPATIBLE_NEW_CLASS(pool, SubDocOperation, GetTimestamp(), mMainPkType, mSubPkType);
    operation->Init(mDocOperateType, mainOperation, subOperations, mSubOperationCount);
    return operation;
}

size_t SubDocOperation::GetMemoryUse() const
{
    size_t totalSize = sizeof(SubDocOperation);
    if (mMainOperation) {
        totalSize += mMainOperation->GetMemoryUse();
    }

    for (size_t i = 0; i < mSubOperationCount; i++) {
        assert(mSubOperations[i]);
        totalSize += mSubOperations[i]->GetMemoryUse();
        totalSize += sizeof(OperationBase*);
    }
    return totalSize;
}

segmentid_t SubDocOperation::GetSegmentId() const
{
    if (mMainOperation) {
        segmentid_t segmentId = mMainOperation->GetSegmentId();
        if (segmentId != INVALID_SEGMENTID) {
            return segmentId;
        }
    }
    if (mSubOperationCount > 0) {
        return mSubOperations[0]->GetSegmentId();
    }
    return INVALID_SEGMENTID;
}

size_t SubDocOperation::GetSerializeSize() const
{
    size_t totalSize = sizeof(uint8_t)                                             // docOperateType
                       + sizeof(uint8_t)                                           // is main operation exist flag
                       + (mMainOperation ? mMainOperation->GetSerializeSize() : 0) // main operation data
                       + VByteCompressor::GetVInt32Length(mSubOperationCount);     // num of sub operations
    for (size_t i = 0; i < mSubOperationCount; ++i) {
        totalSize += mSubOperations[i]->GetSerializeSize();
    }
    return totalSize;
}

size_t SubDocOperation::Serialize(char* buffer, size_t bufferLen) const
{
    char* cur = buffer;
    *(uint8_t*)cur = (uint8_t)mDocOperateType;
    cur += sizeof(uint8_t);
    uint8_t isMainOpExist = mMainOperation ? 1 : 0;
    *(uint8_t*)cur = isMainOpExist;
    cur += sizeof(isMainOpExist);
    if (isMainOpExist) {
        cur += mMainOperation->Serialize(cur, bufferLen - (cur - buffer));
    }
    VByteCompressor::WriteVUInt32(mSubOperationCount, cur);
    for (size_t i = 0; i < mSubOperationCount; ++i) {
        cur += mSubOperations[i]->Serialize(cur, bufferLen - (cur - buffer));
    }
    return cur - buffer;
}

bool SubDocOperation::Load(Pool* pool, char*& cursor)
{
    mDocOperateType = static_cast<DocOperateType>(*(uint8_t*)cursor);
    cursor += sizeof(uint8_t);
    bool isMainOpExist = *(uint8_t*)cursor ? true : false;
    cursor += sizeof(uint8_t);
    if (isMainOpExist) {
        mMainOperation = LoadOperation(pool, cursor, mDocOperateType, mMainPkType);
        if (!mMainOperation) {
            return false;
        }
    }
    mSubOperationCount = VByteCompressor::ReadVUInt32(cursor);
    if (mSubOperationCount > 0) {
        mSubOperations = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, OperationBase*, mSubOperationCount);
        if (!mSubOperations) {
            IE_LOG(ERROR, "allocate memory fail!");
            return false;
        }
    }
    for (size_t i = 0; i < mSubOperationCount; ++i) {
        mSubOperations[i] = LoadOperation(pool, cursor, mDocOperateType, mSubPkType);
        if (!mSubOperations[i]) {
            return false;
        }
    }
    return true;
}

OperationBase* SubDocOperation::LoadOperation(Pool* pool, char*& cursor, DocOperateType docOpType,
                                              InvertedIndexType pkType)
{
    OperationBase* operation = NULL;
    switch (docOpType) {
    case UPDATE_FIELD:
        operation = (pkType == it_primarykey64)
                        ? static_cast<OperationBase*>(
                              IE_POOL_COMPATIBLE_NEW_CLASS(pool, UpdateFieldOperation<uint64_t>, mTimestamp))
                        : static_cast<OperationBase*>(
                              IE_POOL_COMPATIBLE_NEW_CLASS(pool, UpdateFieldOperation<uint128_t>, mTimestamp));
        break;
    case DELETE_SUB_DOC:
        operation =
            (pkType == it_primarykey64)
                ? static_cast<OperationBase*>(IE_POOL_COMPATIBLE_NEW_CLASS(pool, RemoveOperation<uint64_t>, mTimestamp))
                : static_cast<OperationBase*>(
                      IE_POOL_COMPATIBLE_NEW_CLASS(pool, RemoveOperation<uint128_t>, mTimestamp));
        break;
    default:
        assert(false);
        return NULL;
    }
    if (!operation) {
        IE_LOG(ERROR, "allocate memory fail!");
        return NULL;
    }
    if (!operation->Load(pool, cursor)) {
        IE_LOG(ERROR, "load operation fail!");
        return NULL;
    }
    return operation;
}
}} // namespace indexlib::partition
