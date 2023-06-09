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

#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/index/common/block_array/BlockArrayDataAccessor.h"

namespace indexlibv2 { namespace index {

template <typename Key, typename Value>
class BlockArrayCompressDataAccessor : public BlockArrayDataAccessor<Key, Value>
{
public:
    using KVItem = typename BlockArrayDataAccessor<Key, Value>::KVItem;
    using AccessMode = typename BlockArrayDataAccessor<Key, Value>::AccessMode;

    BlockArrayCompressDataAccessor() {}
    ~BlockArrayCompressDataAccessor() {}

    BlockArrayCompressDataAccessor(const BlockArrayCompressDataAccessor&) = delete;
    BlockArrayCompressDataAccessor& operator=(const BlockArrayCompressDataAccessor&) = delete;
    BlockArrayCompressDataAccessor(BlockArrayCompressDataAccessor&&) = delete;
    BlockArrayCompressDataAccessor& operator=(BlockArrayCompressDataAccessor&&) = delete;

public:
    Status Init(const indexlib::file_system::FileReaderPtr& fileReader, uint64_t dataBlockSize) override;

    future_lite::coro::Lazy<indexlib::index::Result<bool>>
    GetValueInBlockAsync(const Key& key, uint64_t blockId, uint64_t keyCountInBlock,
                         indexlib::file_system::ReadOption option, Value* value) const noexcept override;
    AccessMode GetMode() const override { return AccessMode::COMPRESS; }

private:
    indexlib::file_system::CompressFileReaderPtr _compressFileReader;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_2(indexlib.index, BlockArrayCompressDataAccessor, Key, Value);

template <typename Key, typename Value>
Status BlockArrayCompressDataAccessor<Key, Value>::Init(const indexlib::file_system::FileReaderPtr& fileReader,
                                                        uint64_t dataBlockSize)
{
    this->_fileReader = fileReader;
    this->_dataBlockSize = dataBlockSize;
    _compressFileReader = std::dynamic_pointer_cast<indexlib::file_system::CompressFileReader>(this->_fileReader);
    if (!_compressFileReader) {
        RETURN_IF_STATUS_ERROR(Status::ConfigError(), "[%s] should be compress file reader!",
                               fileReader->DebugString().c_str());
    }
    return Status::OK();
}

template <typename Key, typename Value>
inline future_lite::coro::Lazy<indexlib::index::Result<bool>>
BlockArrayCompressDataAccessor<Key, Value>::GetValueInBlockAsync(const Key& key, uint64_t blockId,
                                                                 uint64_t keyCountInBlock,
                                                                 indexlib::file_system::ReadOption option,
                                                                 Value* value) const noexcept
{
    uint64_t dataBlockOffset = this->_dataBlockSize * blockId;
    uint64_t realDataLength = keyCountInBlock * sizeof(KVItem);
    size_t bufferSize = this->_dataBlockSize;
    std::vector<char> dataBuf(bufferSize);
    auto buf = dataBuf.data();

    indexlib::file_system::TemporarySessionCompressFileReader tmpSessionReader(_compressFileReader.get());
    auto sessionReader = tmpSessionReader.Get();

    indexlib::file_system::BatchIO batchIO;
    batchIO.emplace_back(buf, realDataLength, dataBlockOffset);
    auto result = co_await sessionReader->BatchRead(batchIO, option);
    assert(result.size() == 1);
    if (!result[0].OK()) {
        AUTIL_LOG(ERROR, "read data from [%s] fail, offset[%zu], len[%zu]", this->_fileReader->DebugString().c_str(),
                  dataBlockOffset, realDataLength);
        co_return indexlib::index::ConvertFSErrorCode(result[0].ec);
    }
    KVItem* dataStart = (KVItem*)dataBuf.data();
    KVItem* dataEnd = dataStart + keyCountInBlock;
    bool ret = this->LocateItem(dataStart, dataEnd, key, value);
    co_return ret;
}
}} // namespace indexlibv2::index
