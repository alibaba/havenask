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

#include "autil/LongHashValue.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/common/block_array/KeyValueItem.h"
#include "indexlib/index/primary_key/PrimaryKeyFileWriter.h"
#include "indexlib/index/primary_key/PrimaryKeyPair.h"
#include "indexlib/util/HashMap.h"

namespace indexlibv2 { namespace index {

template <typename Key>
class SortedPrimaryKeyFileWriter : public PrimaryKeyFileWriter<Key>
{
public:
    using KVItem = indexlib::index::KeyValueItem<Key, docid_t>;

public:
    SortedPrimaryKeyFileWriter() {}
    ~SortedPrimaryKeyFileWriter() {}

public:
    void Init(size_t docCount, size_t pkCount, const indexlib::file_system::FileWriterPtr& file,
              autil::mem_pool::PoolBase* pool) override;

    Status AddPKPair(Key key, docid_t docid) override;
    Status AddSortedPKPair(Key key, docid_t docid) override;
    Status Close() override;

    int64_t EstimateDumpTempMemoryUse(size_t docCount) override { return docCount * sizeof(KVItem); }

private:
    size_t _pkCount = 0;
    Key _lastSortedKey = 0;
    bool _hasSortedKey = false;
    KVItem* _buffer = nullptr;
    size_t _pkBufferIdx = 0;
    indexlib::file_system::FileWriterPtr _file;
    autil::mem_pool::PoolBase* _pool = nullptr;
    AUTIL_LOG_DECLARE();
    friend class SortedPrimaryKeyFileWriterTest;
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SortedPrimaryKeyFileWriter, T);

template <typename Key>
void SortedPrimaryKeyFileWriter<Key>::Init(size_t docCount, size_t pkCount,
                                           const indexlib::file_system::FileWriterPtr& file,
                                           autil::mem_pool::PoolBase* pool)
{
    _pkCount = pkCount;
    _buffer = nullptr;
    _pool = pool;
    _file = file;
}

template <typename Key>
Status SortedPrimaryKeyFileWriter<Key>::AddPKPair(Key key, docid_t docid)
{
    if (unlikely(!_buffer)) {
        _buffer = IE_POOL_COMPATIBLE_NEW_VECTOR(_pool, KVItem, _pkCount);
    }
    _buffer[_pkBufferIdx].key = key;
    _buffer[_pkBufferIdx].value = docid;
    _pkBufferIdx++;
    return Status::OK();
}

template <typename Key>
Status SortedPrimaryKeyFileWriter<Key>::AddSortedPKPair(Key key, docid_t docid)
{
    if (unlikely(!_hasSortedKey)) {
        _hasSortedKey = true;
        _lastSortedKey = key;
    } else {
        if (unlikely(key < _lastSortedKey)) {
            AUTIL_LOG(ERROR, "add sort key failed, key not sorted.");
            return Status::Corruption("add sort key failed, key not sorted");
        }
    }
    KVItem item;
    item.key = key;
    item.value = docid;
    auto [status, writeSize] = _file->Write(&item, sizeof(item)).StatusWith();
    return status;
}

template <typename Key>
Status SortedPrimaryKeyFileWriter<Key>::Close()
{
    if (_buffer) {
        std::sort(_buffer, _buffer + _pkCount);
        size_t bufLen = sizeof(KVItem) * _pkCount;
        _file->ReserveFile(bufLen).GetOrThrow();
        auto [status, writeSize] = _file->Write(_buffer, bufLen).StatusWith();
        if (!status.IsOK()) {
            return status;
        }
        assert(_file->GetLength() == bufLen);
        IE_POOL_COMPATIBLE_DELETE_VECTOR(_pool, _buffer, _pkCount);
    }
    return _file->Close().Status();
}

}} // namespace indexlibv2::index
