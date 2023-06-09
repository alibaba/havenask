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

#include "autil/LongHashValue.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/common/block_array/BlockArrayWriter.h"
#include "indexlib/index/common/block_array/KeyValueItem.h"
#include "indexlib/index/primary_key/PrimaryKeyFileWriter.h"
#include "indexlib/index/primary_key/PrimaryKeyPair.h"
#include "indexlib/util/HashMap.h"
#include "indexlib/util/SimplePool.h"

namespace indexlibv2 { namespace index {

template <typename Key>
class BlockPrimaryKeyFileWriter : public PrimaryKeyFileWriter<Key>
{
public:
    using BlockArrayWriterPtr = std::shared_ptr<BlockArrayWriter<Key, docid_t>>;
    using KVItem = indexlib::index::KeyValueItem<Key, docid_t>;
    BlockPrimaryKeyFileWriter(int32_t pkDataBlockSize) : _pkDataBlockSize(pkDataBlockSize) {}
    ~BlockPrimaryKeyFileWriter() {}

public:
    void Init(size_t docCount, size_t pkCount, const indexlib::file_system::FileWriterPtr& file,
              autil::mem_pool::PoolBase* pool) override;

    Status AddPKPair(Key key, docid_t docid) override;
    Status AddSortedPKPair(Key key, docid_t docid) override;
    Status Close() override;

    int64_t EstimateDumpTempMemoryUse(size_t docCount) override { return docCount * sizeof(KVItem); }

private:
    int32_t _pkDataBlockSize;
    KVItem* _buffer = nullptr;
    size_t _pkBufferIdx = 0;
    size_t _pkCount = 0;
    Key _lastSortedKey = 0;
    bool _hasSortedKey = false;
    indexlib::file_system::FileWriterPtr _file;
    std::shared_ptr<BlockArrayWriter<Key, docid_t>> _blockArrayWriter;
    autil::mem_pool::PoolBase* _pool = nullptr;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, BlockPrimaryKeyFileWriter, T);

template <typename Key>
void BlockPrimaryKeyFileWriter<Key>::Init(size_t docCount, size_t pkCount,
                                          const indexlib::file_system::FileWriterPtr& file,
                                          autil::mem_pool::PoolBase* pool)
{
    _pkCount = pkCount;
    _buffer = nullptr;
    _blockArrayWriter.reset(new BlockArrayWriter<Key, docid_t>(pool));
    _blockArrayWriter->Init(_pkDataBlockSize, file);
    _file = file;
    _pool = pool;
}

template <typename Key>
Status BlockPrimaryKeyFileWriter<Key>::AddPKPair(Key key, docid_t docid)
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
Status BlockPrimaryKeyFileWriter<Key>::AddSortedPKPair(Key key, docid_t docid)
{
    if (unlikely(!_hasSortedKey)) {
        _hasSortedKey = true;
        _lastSortedKey = key;
    } else {
        if (unlikely(key < _lastSortedKey)) {
            AUTIL_LOG(ERROR, "add sort key failed, key not sorted");
            return Status::Corruption("add sort key failed, key not sorted");
        }
    }
    return _blockArrayWriter->AddItem(key, docid);
}

template <typename Key>
Status BlockPrimaryKeyFileWriter<Key>::Close()
{
    if (_buffer) {
        std::sort(_buffer, _buffer + _pkCount);
        for (size_t i = 0; i < _pkCount; i++) {
            auto status = _blockArrayWriter->AddItem(_buffer[i].key, _buffer[i].value);
            RETURN_IF_STATUS_ERROR(status, "block array writer add item fail");
        }
        IE_POOL_COMPATIBLE_DELETE_VECTOR(_pool, _buffer, _pkCount);
    }
    auto status = _blockArrayWriter->Finish();
    RETURN_IF_STATUS_ERROR(status, "block array writter finish failed, file[%s]", _file->DebugString().c_str());
    _blockArrayWriter.reset();
    return _file->Close().Status();
}

}} // namespace indexlibv2::index
