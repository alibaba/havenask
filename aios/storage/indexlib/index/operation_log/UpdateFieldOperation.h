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

#include "autil/ConstString.h"
#include "autil/LongHashValue.h"
#include "indexlib/base/Status.h"
#include "indexlib/document/normal/ModifiedTokens.h"
#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"
#include "indexlib/index/operation_log/OperationBase.h"
#include "indexlib/index/operation_log/OperationLogProcessor.h"
#include "indexlib/index/operation_log/OperationRedoHint.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/util/PoolUtil.h"

namespace indexlib::index {
using OperationItem = std::pair<fieldid_t, autil::StringView>;

template <typename T>
class UpdateFieldOperation : public OperationBase
{
public:
    UpdateFieldOperation(int64_t timestamp, uint16_t hashId);
    ~UpdateFieldOperation() = default;

public:
    void Init(T pk, OperationItem* items, size_t itemSize, segmentid_t segmentId);
    bool Load(autil::mem_pool::Pool* pool, char*& cursor) override;
    OperationBase* Clone(autil::mem_pool::Pool* pool) override;
    SerializedOperationType GetSerializedType() const override { return UPDATE_FIELD_OP; }
    DocOperateType GetDocOperateType() const override { return UPDATE_FIELD; }
    size_t GetMemoryUse() const override;
    segmentid_t GetSegmentId() const override { return _segmentId; }
    size_t GetSerializeSize() const override;
    size_t Serialize(char* buffer, size_t bufferLen) const override;
    const T& GetPkHash() const { return _pkHash; }
    bool Process(const PrimaryKeyIndexReader* pkReader, OperationLogProcessor* processor,
                 const std::vector<std::pair<docid_t, docid_t>>& docidRange) const override;
    size_t GetItemSize() { return _itemSize; }
    OperationItem GetOperationItem(size_t itemIdx) { return _items[itemIdx]; }
    const char* GetPkHashPointer() const override { return (char*)(&_pkHash); }

private:
    T _pkHash;
    OperationItem* _items;
    uint32_t _itemSize;
    segmentid_t _segmentId;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, UpdateFieldOperation, T);

template <typename T>
UpdateFieldOperation<T>::UpdateFieldOperation(int64_t timestamp, uint16_t hashId)
    : OperationBase(timestamp, hashId)
    , _pkHash(T())
    , _items(nullptr)
    , _itemSize(0)
    , _segmentId(INVALID_SEGMENTID)
{
}

template <typename T>
void UpdateFieldOperation<T>::Init(T pk, OperationItem* items, size_t itemSize, segmentid_t segmentId)
{
    assert(items != nullptr);
    assert(itemSize > 0);

    _pkHash = pk;
    _items = items;
    _itemSize = itemSize;
    _segmentId = segmentId;
}

template <typename T>
bool UpdateFieldOperation<T>::Process(const PrimaryKeyIndexReader* pkIndexReader, OperationLogProcessor* processor,
                                      const std::vector<std::pair<docid_t, docid_t>>& docIdRanges) const
{
    assert(processor != nullptr);
    assert(pkIndexReader != nullptr);
    for (const auto& docIdRange : docIdRanges) {
        docid_t docId = pkIndexReader->LookupWithDocRange(_pkHash, docIdRange, /*executor*/ nullptr);
        if (docId == INVALID_DOCID) {
            continue;
        }
        bool isAfterSeparator = false;
        for (uint32_t i = 0; i < _itemSize; i++) {
            const auto& item = _items[i];
            fieldid_t fieldId = item.first;
            if (fieldId == INVALID_FIELDID) {
                isAfterSeparator = true;
                continue;
            }
            if (!isAfterSeparator) {
                // attribute
                const std::string& fieldName = _operationFieldInfo->GetFieldName(fieldId);
                if (unlikely(fieldName.empty())) {
                    continue;
                }
                const autil::StringView& value = item.second;
                bool ret = processor->UpdateFieldValue(docId, fieldName, value, value.empty());
                if (!ret) {
                    AUTIL_INTERVAL_LOG2(2, ERROR, "process update field op for doc [%d] failed", docId);
                }
            } else {
                // inverted index
                autil::DataBuffer buffer((char*)item.second.data(), item.second.size());
                document::ModifiedTokens modifiedTokens;
                modifiedTokens.Deserialize(buffer);
                auto status = processor->UpdateFieldTokens(docId, modifiedTokens);
                if (not status.IsOK()) {
                    AUTIL_LOG(WARN, "process update field op for doc [%d] failed", docId);
                }
            }
        }
        return true;
    }
    return true;
}

template <typename T>
size_t UpdateFieldOperation<T>::GetSerializeSize() const
{
    uint32_t itemSize = _itemSize;
    size_t totalSize = sizeof(GetPkHash()) + sizeof(segmentid_t) + VByteCompressor::GetVInt32Length(itemSize);

    for (uint32_t i = 0; i < itemSize; i++) {
        const OperationItem& item = _items[i];
        fieldid_t fieldId = item.first;
        uint32_t valueSize = item.second.size();
        totalSize +=
            VByteCompressor::GetVInt32Length(fieldId) + VByteCompressor::GetVInt32Length(valueSize) + valueSize;
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
    uint32_t itemSize = _itemSize;
    VByteCompressor::WriteVUInt32(itemSize, cur);
    for (uint32_t i = 0; i < itemSize; i++) {
        const OperationItem& item = _items[i];
        VByteCompressor::WriteVUInt32(item.first, cur);
        uint32_t valueLen = item.second.size();
        VByteCompressor::WriteVUInt32(valueLen, cur);
        memcpy(cur, item.second.data(), valueLen);
        cur += valueLen;
    }
    return cur - buffer;
}

template <typename T>
bool UpdateFieldOperation<T>::Load(autil::mem_pool::Pool* pool, char*& cursor)
{
    this->_pkHash = *(T*)cursor;
    cursor += sizeof(T);
    _segmentId = (*(segmentid_t*)cursor);
    cursor += sizeof(segmentid_t);
    _itemSize = VByteCompressor::ReadVUInt32(cursor);
    OperationItem* items = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, OperationItem, _itemSize);
    if (!items) {
        AUTIL_LOG(ERROR, "allocate memory fail!");
        return false;
    }
    _items = items;
    for (uint32_t i = 0; i < _itemSize; ++i) {
        fieldid_t fieldId = VByteCompressor::ReadVUInt32(cursor);
        // instead allocate space from pool, item value reuse fileNode's memory buffer
        uint32_t valueLen = VByteCompressor::ReadVUInt32(cursor);
        _items[i] = std::make_pair(fieldId, autil::StringView(cursor, valueLen));
        cursor += valueLen;
    }
    // load index
    return true;
}

template <typename T>
OperationBase* UpdateFieldOperation<T>::Clone(autil::mem_pool::Pool* pool)
{
    OperationItem* clonedItems = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, OperationItem, _itemSize);
    if (!clonedItems) {
        AUTIL_LOG(ERROR, "allocate memory fail!");
        return NULL;
    }

    for (uint32_t i = 0; i < _itemSize; ++i) {
        const OperationItem& item = _items[i];
        const autil::StringView& srcData = item.second;
        autil::StringView copyDataValue = autil::MakeCString(srcData.data(), srcData.size(), pool);

        clonedItems[i].first = item.first;
        clonedItems[i].second = copyDataValue;
    }

    UpdateFieldOperation* clonedOperation =
        IE_POOL_COMPATIBLE_NEW_CLASS(pool, UpdateFieldOperation, GetTimestamp(), _hashId);
    clonedOperation->Init(GetPkHash(), clonedItems, _itemSize, GetSegmentId());
    if (!clonedOperation) {
        AUTIL_LOG(ERROR, "allocate memory fail!");
        return NULL;
    }
    return clonedOperation;
}

template <typename T>
size_t UpdateFieldOperation<T>::GetMemoryUse() const
{
    size_t totalSize = sizeof(UpdateFieldOperation);
    for (uint32_t i = 0; i < _itemSize; i++) {
        totalSize += sizeof(OperationItem);
        totalSize += _items[i].second.size();
    }
    return totalSize;
}

} // namespace indexlib::index
