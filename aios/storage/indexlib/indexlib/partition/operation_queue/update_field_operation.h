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

#include <memory>

#include "autil/ConstString.h"
#include "autil/LongHashValue.h"
#include "indexlib/common_define.h"
#include "indexlib/document/normal/ModifiedTokens.h"
#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/operation_queue/operation_base.h"

namespace indexlib { namespace partition {

typedef std::pair<fieldid_t, autil::StringView> OperationItem;

template <typename T>
class UpdateFieldOperation : public OperationBase
{
public:
    UpdateFieldOperation(int64_t timestamp);
    ~UpdateFieldOperation() {}

public:
    void Init(T pk, OperationItem* items, size_t itemSize, segmentid_t segmentId);
    bool Process(const partition::PartitionModifierPtr& modifier, const OperationRedoHint& redoHint,
                 future_lite::Executor* executor) override;

    DocOperateType GetDocOperateType() const override { return UPDATE_FIELD; }
    OperationBase::SerializedOperationType GetSerializedType() const override { return OperationBase::UPDATE_FIELD_OP; }

    OperationBase* Clone(autil::mem_pool::Pool* pool) override;

    size_t GetSerializeSize() const override;
    size_t Serialize(char* buffer, size_t bufferLen) const override;

    bool Load(autil::mem_pool::Pool* pool, char*& cursor) override;

public:
    T GetPkHash() const { return mPkHash; }
    void SetPKHash(T PkHash) { mPkHash = PkHash; }
    const OperationItem& GetOperationItem(uint32_t i) const { return mItems[i]; }
    void SetOperationItem(uint32_t i, const OperationItem& item) { mItems[i] = item; }
    void SetOperationItems(OperationItem* items) { mItems = items; }
    uint32_t GetItemSize() const { return mItemSize; }
    void SetItemSize(uint32_t itemSize) { mItemSize = itemSize; }
    segmentid_t GetSegmentId() const override { return mSegmentId; }
    void SetSegmentId(segmentid_t segmentId) { mSegmentId = segmentId; }
    size_t GetMemoryUse() const override;

private:
    void UpdateOneDoc(const partition::PartitionModifierPtr& modifier, docid_t docId);
    OperationItem& GetOperationItem(uint32_t i) { return mItems[i]; }
    bool UpdateDocInDocRange(const partition::PartitionModifierPtr& modifier,
                             const index::PrimaryKeyIndexReaderPtr& pkReader,
                             const std::vector<std::pair<docid_t, docid_t>>& docRanges, bool doAll);

private:
    T mPkHash;
    OperationItem* mItems;
    uint32_t mItemSize;
    segmentid_t mSegmentId;
    future_lite::Executor* mExecutor;

private:
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////////

IE_LOG_SETUP_TEMPLATE(partition, UpdateFieldOperation);

template <typename T>
UpdateFieldOperation<T>::UpdateFieldOperation(int64_t timestamp)
    : OperationBase(timestamp)
    , mPkHash(T())
    , mItems(NULL)
    , mItemSize(0)
    , mSegmentId(INVALID_SEGMENTID)
{
}

template <typename T>
void UpdateFieldOperation<T>::Init(T pk, OperationItem* items, size_t itemSize, segmentid_t segmentId)
{
    assert(items);
    assert(itemSize > 0);

    mPkHash = pk;
    mItems = items;
    mItemSize = itemSize;
    mSegmentId = segmentId;
}

template <typename T>
bool UpdateFieldOperation<T>::Process(const partition::PartitionModifierPtr& modifier,
                                      const OperationRedoHint& redoHint, future_lite::Executor* executor)
{
    assert(modifier);
    mExecutor = executor;
    const index::PrimaryKeyIndexReaderPtr& pkIndexReader = modifier->GetPrimaryKeyIndexReader();
    assert(pkIndexReader);

    auto processOneSegmentIdFunc = [&](OperationRedoHint::HintType type, segmentid_t segId) -> bool {
        docid_t docId = pkIndexReader->LookupWithPKHash(mPkHash, mExecutor);
        if (docId == INVALID_DOCID) {
            IE_LOG(DEBUG, "get docid from pkReader invalid.");
            return false;
        }
        if (type == OperationRedoHint::HintType::REDO_USE_HINT_DOC ||
            type == OperationRedoHint::HintType::REDO_CACHE_SEGMENT) {
            std::pair<segmentid_t, docid_t> docInfo = modifier->GetPartitionInfo()->GetLocalDocInfo(docId);
            if (docInfo.first != segId) {
                // for case AUA, add doc in a segment, then update and add the same doc in the other segment
                // so the operation no need redo
                IE_LOG(DEBUG,
                       "no need redo operation, doc has been deleleted in segId[%d], new docId[%d] current segid[%d].",
                       segId, docInfo.first, segId);
                return true;
            }
        }
        UpdateOneDoc(modifier, docId);
        return true;
    };

    if (!redoHint.IsValid()) {
        return processOneSegmentIdFunc(redoHint.GetHintType(), redoHint.GetSegmentId());
    }

    if (redoHint.GetHintType() == OperationRedoHint::HintType::REDO_DOC_RANGE) {
        bool doAll = true;
        return UpdateDocInDocRange(modifier, pkIndexReader, redoHint.GetCachedDocRange().first, doAll);
    } else if (redoHint.GetHintType() == OperationRedoHint::HintType::REDO_CACHE_SEGMENT) {
        auto cachedSegmentIds = redoHint.GetCachedSegmentIds();
        assert(cachedSegmentIds.size() == 1);
        return processOneSegmentIdFunc(redoHint.GetHintType(), cachedSegmentIds[0]);
    }
    assert(false);
    // unexpected to be reached
    return false;
}

template <typename T>
bool UpdateFieldOperation<T>::UpdateDocInDocRange(const partition::PartitionModifierPtr& modifier,
                                                  const index::PrimaryKeyIndexReaderPtr& pkReader,
                                                  const std::vector<std::pair<docid_t, docid_t>>& docRanges, bool doAll)
{
    for (auto range : docRanges) {
        docid_t docId = pkReader->LookupWithDocRange(mPkHash, range, mExecutor);
        if (docId != INVALID_DOCID) {
            UpdateOneDoc(modifier, docId);
            if (!doAll) {
                return true;
            }
        }
    }
    return true;
}

template <typename T>
size_t UpdateFieldOperation<T>::GetSerializeSize() const
{
    uint32_t itemSize = GetItemSize();
    size_t totalSize = sizeof(GetPkHash()) + sizeof(segmentid_t) + index::VByteCompressor::GetVInt32Length(itemSize);

    for (uint32_t i = 0; i < itemSize; i++) {
        const OperationItem& item = GetOperationItem(i);
        fieldid_t fieldId = item.first;
        uint32_t valueSize = item.second.size();
        totalSize += index::VByteCompressor::GetVInt32Length(fieldId) +
                     index::VByteCompressor::GetVInt32Length(valueSize) + valueSize;
    }
    return totalSize;
}

template <typename T>
size_t UpdateFieldOperation<T>::Serialize(char* buffer, size_t bufferLen) const
{
    assert(bufferLen >= GetSerializeSize());
    char* cur = buffer;
    *(T*)cur = GetPkHash();
    cur += sizeof(T);
    *(segmentid_t*)cur = GetSegmentId();
    cur += sizeof(segmentid_t);
    uint32_t itemSize = GetItemSize();
    index::VByteCompressor::WriteVUInt32(itemSize, cur);
    for (uint32_t i = 0; i < itemSize; i++) {
        const OperationItem& item = GetOperationItem(i);
        index::VByteCompressor::WriteVUInt32(item.first, cur);
        uint32_t valueLen = item.second.size();
        index::VByteCompressor::WriteVUInt32(valueLen, cur);
        memcpy(cur, item.second.data(), valueLen);
        cur += valueLen;
    }
    return cur - buffer;
}

template <typename T>
bool UpdateFieldOperation<T>::Load(autil::mem_pool::Pool* pool, char*& cursor)
{
    SetPKHash(*(T*)cursor);
    cursor += sizeof(T);
    SetSegmentId(*(segmentid_t*)cursor);
    cursor += sizeof(segmentid_t);
    uint32_t itemSize = index::VByteCompressor::ReadVUInt32(cursor);
    SetItemSize(itemSize);
    OperationItem* items = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, OperationItem, itemSize);
    if (!items) {
        IE_LOG(ERROR, "allocate memory fail!");
        return false;
    }
    SetOperationItems(items);

    for (uint32_t i = 0; i < GetItemSize(); ++i) {
        fieldid_t fieldId = index::VByteCompressor::ReadVUInt32(cursor);
        // instead allocate space from pool, item value reuse fileNode's memory buffer
        uint32_t valueLen = index::VByteCompressor::ReadVUInt32(cursor);
        SetOperationItem(i, std::make_pair(fieldId, autil::StringView(cursor, valueLen)));
        cursor += valueLen;
    }
    // load index
    return true;
}

template <typename T>
OperationBase* UpdateFieldOperation<T>::Clone(autil::mem_pool::Pool* pool)
{
    uint32_t itemSize = GetItemSize();
    OperationItem* clonedItems = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, OperationItem, itemSize);
    if (!clonedItems) {
        IE_LOG(ERROR, "allocate memory fail!");
        return NULL;
    }

    for (uint32_t i = 0; i < itemSize; ++i) {
        const OperationItem& item = GetOperationItem(i);
        const autil::StringView& srcData = item.second;
        autil::StringView copyDataValue = autil::MakeCString(srcData.data(), srcData.size(), pool);

        clonedItems[i].first = item.first;
        clonedItems[i].second = copyDataValue;
    }

    UpdateFieldOperation* clonedOperation = IE_POOL_COMPATIBLE_NEW_CLASS(pool, UpdateFieldOperation, GetTimestamp());
    clonedOperation->Init(GetPkHash(), clonedItems, itemSize, GetSegmentId());
    if (!clonedOperation) {
        IE_LOG(ERROR, "allocate memory fail!");
        return NULL;
    }
    return clonedOperation;
}

template <typename T>
void UpdateFieldOperation<T>::UpdateOneDoc(const partition::PartitionModifierPtr& modifier, docid_t docId)
{
    bool afterSep = false;
    for (uint32_t i = 0; i < GetItemSize(); i++) {
        OperationItem& item = GetOperationItem(i);
        fieldid_t fieldId = item.first;
        if (fieldId == INVALID_FIELDID) {
            afterSep = true;
            continue;
        }
        if (afterSep) {
            // inverted index
            autil::DataBuffer buffer((char*)item.second.data(), item.second.size());
            document::ModifiedTokens modifiedTokens;
            modifiedTokens.Deserialize(buffer);
            modifier->RedoIndex(docId, modifiedTokens);
        } else {
            const autil::StringView& value = item.second;
            modifier->UpdateField(docId, fieldId, value, value.empty());
        }
    }
}

template <typename T>
size_t UpdateFieldOperation<T>::GetMemoryUse() const
{
    size_t totalSize = sizeof(UpdateFieldOperation);
    for (uint32_t i = 0; i < GetItemSize(); i++) {
        totalSize += sizeof(OperationItem);
        totalSize += GetOperationItem(i).second.size();
    }
    return totalSize;
}
}} // namespace indexlib::partition
