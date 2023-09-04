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

#include "indexlib/base/Progress.h"
#include "indexlib/index/operation_log/OperationBase.h"
#include "indexlib/index/operation_log/OperationLogProcessor.h"
#include "indexlib/index/operation_log/OperationRedoHint.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/util/PoolUtil.h"

namespace indexlib::index {

template <typename T>
class RemoveOperation : public OperationBase
{
public:
    RemoveOperation(const indexlibv2::document::IDocument::DocInfo& docInfo) : OperationBase(docInfo) {}
    ~RemoveOperation() {}

public:
    void Init(T pk, segmentid_t segmentId);
    bool Load(autil::mem_pool::Pool* pool, char*& cursor) override;
    OperationBase* Clone(autil::mem_pool::Pool* pool) override;
    SerializedOperationType GetSerializedType() const override { return REMOVE_OP; }
    DocOperateType GetDocOperateType() const override { return DELETE_DOC; }
    size_t GetMemoryUse() const override { return sizeof(RemoveOperation); }
    segmentid_t GetSegmentId() const override { return _segmentId; }
    size_t GetSerializeSize() const override { return sizeof(_pkHash) + sizeof(_segmentId); }
    size_t Serialize(char* buffer, size_t bufferLen) const override;
    const T& GetPkHash() const { return _pkHash; }
    bool Process(const PrimaryKeyIndexReader* pkReader, OperationLogProcessor* processor,
                 const std::vector<std::pair<docid_t, docid_t>>& docidRange) const override;
    const char* GetPkHashPointer() const override { return (char*)(&_pkHash); }

private:
    T _pkHash;
    segmentid_t _segmentId;

private:
    friend class RemoveOperationTest_TestClone_Test;
    friend class RemoveOperationTest_TestSerialize_Test;
    friend class RemoveOperationCreatorTest_TestCreate_Test;
};

template <typename T>
void RemoveOperation<T>::Init(T pk, segmentid_t segmentId)
{
    _pkHash = pk;
    _segmentId = segmentId;
}

template <typename T>
bool RemoveOperation<T>::Load(autil::mem_pool::Pool* pool, char*& cursor)
{
    _pkHash = *(T*)cursor;
    cursor += sizeof(T);
    _segmentId = *(segmentid_t*)cursor;
    cursor += sizeof(_segmentId);
    return true;
}

template <typename T>
OperationBase* RemoveOperation<T>::Clone(autil::mem_pool::Pool* pool)
{
    RemoveOperation* clonedOperation = IE_POOL_COMPATIBLE_NEW_CLASS(pool, RemoveOperation, _docInfo);
    clonedOperation->Init(_pkHash, _segmentId);
    return clonedOperation;
}

template <typename T>
size_t RemoveOperation<T>::Serialize(char* buffer, size_t bufferLen) const
{
    assert(bufferLen >= GetSerializeSize());
    char* cur = buffer;
    *(T*)cur = _pkHash;
    cur += sizeof(T);
    *(segmentid_t*)cur = _segmentId;
    cur += sizeof(_segmentId);
    return cur - buffer;
}
template <typename T>
bool RemoveOperation<T>::Process(const PrimaryKeyIndexReader* pkReader, OperationLogProcessor* processor,
                                 const std::vector<std::pair<docid_t, docid_t>>& docidRanges) const
{
    // TODO(mokauo.mnb) add executor
    for (const auto& docidRange : docidRanges) {
        auto docId = pkReader->LookupWithDocRange(_pkHash, docidRange, nullptr);
        if (docId == INVALID_DOCID) {
            continue;
        }
        auto status = processor->RemoveDocument(docId);
        return status.IsOK();
    }
    return true;
}

} // namespace indexlib::index
