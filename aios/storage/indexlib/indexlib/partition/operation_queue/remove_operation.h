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
#ifndef __INDEXLIB_REMOVE_OPERATION_H
#define __INDEXLIB_REMOVE_OPERATION_H

#include <memory>

#include "autil/LongHashValue.h"
#include "indexlib/common_define.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/operation_queue/operation_base.h"

namespace indexlib { namespace partition {

template <typename T>
class RemoveOperation : public OperationBase
{
public:
    RemoveOperation(int64_t timestamp);
    ~RemoveOperation() {}

public:
    void Init(T pk, segmentid_t segmentId);

    bool Load(autil::mem_pool::Pool* pool, char*& cursor) override;

    bool Process(const partition::PartitionModifierPtr& modifier, const OperationRedoHint& redoHint,
                 future_lite::Executor* executor) override;

    OperationBase* Clone(autil::mem_pool::Pool* pool) override;

    SerializedOperationType GetSerializedType() const override { return REMOVE_OP; }

    DocOperateType GetDocOperateType() const override { return DELETE_DOC; }

    size_t GetMemoryUse() const override { return sizeof(RemoveOperation); }

    segmentid_t GetSegmentId() const override { return mSegmentId; }

    size_t GetSerializeSize() const override { return sizeof(mPkHash) + sizeof(mSegmentId); }

    size_t Serialize(char* buffer, size_t bufferLen) const override;

    const T& GetPkHash() const { return mPkHash; }

private:
    bool DeleteDocInCachedSegments(const partition::PartitionModifierPtr& modifier,
                                   const index::PrimaryKeyIndexReaderPtr& pkReader,
                                   const std::vector<segmentid_t>& cacheSegments, bool doAll);
    bool DeleteDocInDocRange(const partition::PartitionModifierPtr& modifier,
                             const index::PrimaryKeyIndexReaderPtr& pkReader,
                             const std::vector<std::pair<docid_t, docid_t>>& cachedRanges,
                             future_lite::Executor* executor, bool doAll);

private:
    T mPkHash;
    segmentid_t mSegmentId;

private:
    friend class RemoveOperationTest;
    friend class RemoveOperationCreatorTest;
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////////

IE_LOG_SETUP_TEMPLATE(partition, RemoveOperation);

template <typename T>
RemoveOperation<T>::RemoveOperation(int64_t timestamp)
    : OperationBase(timestamp)
    , mPkHash(T())
    , mSegmentId(INVALID_SEGMENTID)
{
}

template <typename T>
void RemoveOperation<T>::Init(T pk, segmentid_t segmentId)
{
    mPkHash = pk;
    mSegmentId = segmentId;
}

template <typename T>
bool RemoveOperation<T>::Process(const partition::PartitionModifierPtr& modifier, const OperationRedoHint& redoHint,
                                 future_lite::Executor* executor)
{
    assert(modifier);
    const index::PartitionInfoPtr& partInfo = modifier->GetPartitionInfo();
    assert(partInfo);

    docid_t docId = INVALID_DOCID;
    segmentid_t segId = INVALID_SEGMENTID;
    const index::PrimaryKeyIndexReaderPtr& pkIndexReader = modifier->GetPrimaryKeyIndexReader();
    if (!redoHint.IsValid()) {
        assert(pkIndexReader);
        docId = pkIndexReader->LookupWithPKHash(mPkHash, executor);
        modifier->RemoveDocument(docId);
        return true;
    }
    if (redoHint.GetHintType() == OperationRedoHint::HintType::REDO_CACHE_SEGMENT) {
        bool doAll = redoHint.GetHintType() == OperationRedoHint::HintType::REDO_CACHE_SEGMENT;
        return DeleteDocInCachedSegments(modifier, pkIndexReader, redoHint.GetCachedSegmentIds(), doAll);
    } else if (redoHint.GetHintType() == OperationRedoHint::HintType::REDO_DOC_RANGE) {
        bool doAll = true;
        return DeleteDocInDocRange(modifier, pkIndexReader, redoHint.GetCachedDocRange().first, executor, doAll);
    }
    assert(redoHint.GetHintType() == OperationRedoHint::HintType::REDO_USE_HINT_DOC);
    segId = redoHint.GetSegmentId();
    docId = partInfo->GetBaseDocId(segId) + redoHint.GetLocalDocId();
    modifier->RemoveDocument(docId);
    return true;
}

template <typename T>
bool RemoveOperation<T>::DeleteDocInCachedSegments(const partition::PartitionModifierPtr& modifier,
                                                   const index::PrimaryKeyIndexReaderPtr& pkReader,
                                                   const std::vector<segmentid_t>& cacheSegments, bool doAll)
{
    for (size_t i = 0; i < cacheSegments.size(); i++) {
        docid_t docid = INVALID_DOCID;
        pkReader->LookupWithPKHash(mPkHash, cacheSegments[i], &docid);
        if (docid != INVALID_DOCID) {
            modifier->RemoveDocument(docid);
            if (!doAll) {
                return true;
            }
        }
    }
    return true;
}

template <typename T>
bool RemoveOperation<T>::DeleteDocInDocRange(const partition::PartitionModifierPtr& modifier,
                                             const index::PrimaryKeyIndexReaderPtr& pkReader,
                                             const std::vector<std::pair<docid_t, docid_t>>& cachedRanges,
                                             future_lite::Executor* executor, bool doAll)
{
    for (auto range : cachedRanges) {
        docid_t docId = pkReader->LookupWithDocRange(mPkHash, range, executor);
        if (docId != INVALID_DOCID) {
            modifier->RemoveDocument(docId);
            if (!doAll) {
                return true;
            }
        }
    }
    return true;
}

template <typename T>
OperationBase* RemoveOperation<T>::Clone(autil::mem_pool::Pool* pool)
{
    RemoveOperation* clonedOperation = IE_POOL_COMPATIBLE_NEW_CLASS(pool, RemoveOperation, mTimestamp);
    clonedOperation->Init(mPkHash, mSegmentId);
    if (!clonedOperation) {
        IE_LOG(ERROR, "allocate memory fail!");
        return NULL;
    }
    return clonedOperation;
}

template <typename T>
size_t RemoveOperation<T>::Serialize(char* buffer, size_t bufferLen) const
{
    assert(bufferLen >= GetSerializeSize());
    char* cur = buffer;
    *(T*)cur = mPkHash;
    cur += sizeof(T);
    *(segmentid_t*)cur = mSegmentId;
    cur += sizeof(mSegmentId);
    return cur - buffer;
}

template <typename T>
bool RemoveOperation<T>::Load(autil::mem_pool::Pool* pool, char*& cursor)
{
    mPkHash = *(T*)cursor;
    cursor += sizeof(T);
    mSegmentId = *(segmentid_t*)cursor;
    cursor += sizeof(mSegmentId);
    return true;
}
}} // namespace indexlib::partition

#endif //__INDEXLIB_REMOVE_OPERATION_H
