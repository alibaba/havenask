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

#include "indexlib/file_system/file/BlockFileNode.h"
#include "indexlib/index/common/block_array/BlockArrayDataAccessor.h"
namespace indexlibv2 { namespace index {

template <typename Key, typename Value>
class BlockArrayCacheDataAccessor : public BlockArrayDataAccessor<Key, Value>
{
public:
    using KVItem = typename BlockArrayDataAccessor<Key, Value>::KVItem;
    using AccessMode = typename BlockArrayDataAccessor<Key, Value>::AccessMode;
    BlockArrayCacheDataAccessor() {}
    virtual ~BlockArrayCacheDataAccessor() {}

    BlockArrayCacheDataAccessor(const BlockArrayCacheDataAccessor&) = delete;
    BlockArrayCacheDataAccessor& operator=(const BlockArrayCacheDataAccessor&) = delete;
    BlockArrayCacheDataAccessor(BlockArrayCacheDataAccessor&&) = delete;
    BlockArrayCacheDataAccessor& operator=(BlockArrayCacheDataAccessor&&) = delete;

public:
    Status Init(const indexlib::file_system::FileReaderPtr& fileReader, uint64_t dataBlockSize) override;

    inline future_lite::coro::Lazy<indexlib::index::Result<bool>>
    GetValueInBlockAsync(const Key& key, uint64_t blockId, uint64_t keyCountInBlock,
                         indexlib::file_system::ReadOption option, Value* value) const noexcept override;
    AccessMode GetMode() const override { return AccessMode::CACHE; }

private:
    indexlib::file_system::BlockFileNodePtr _blockFileNode; // used for reading block on disk

    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP_TEMPLATE_2(indexlib.index, BlockArrayCacheDataAccessor, Key, Value);

template <typename Key, typename Value>
Status BlockArrayCacheDataAccessor<Key, Value>::Init(const indexlib::file_system::FileReaderPtr& fileReader,
                                                     uint64_t dataBlockSize)
{
    this->_fileReader = fileReader;
    this->_dataBlockSize = dataBlockSize;
    _blockFileNode = std::dynamic_pointer_cast<indexlib::file_system::BlockFileNode>(this->_fileReader->GetFileNode());
    if (!_blockFileNode) {
        RETURN_IF_STATUS_ERROR(Status::ConfigError(), "open [%s] with wrong open type [%d], should be FSOT_CACHE",
                               fileReader->DebugString().c_str(), this->_fileReader->GetOpenType());
    }
    return Status::OK();
}

template <typename Key, typename Value>
inline future_lite::coro::Lazy<indexlib::index::Result<bool>>
BlockArrayCacheDataAccessor<Key, Value>::GetValueInBlockAsync(const Key& key, uint64_t blockId,
                                                              uint64_t keyCountInBlock,
                                                              indexlib::file_system::ReadOption option,
                                                              Value* value) const noexcept
{
    indexlib::file_system::BlockFileAccessor* accessor = _blockFileNode->GetAccessor();
    assert(accessor);

    // key's block (data block) may not be aligned with cache block, blockCount may not be 1
    uint64_t dataBlockOffset = this->_dataBlockSize * blockId;
    uint64_t realDataLength = keyCountInBlock * sizeof(KVItem);
    uint64_t blockCount = accessor->GetBlockCount(dataBlockOffset, realDataLength);
    if (blockCount == 1) {
        // optimize: no copy from cache
        size_t blockIdx = accessor->GetBlockIdx(dataBlockOffset);
        std::vector<size_t> blockIdxs(1, blockIdx);
        auto getHandlesRet = co_await accessor->GetBlockHandles(blockIdxs, option);
        assert(getHandlesRet.size() == 1);
        if (!getHandlesRet[0].OK()) {
            AUTIL_LOG(ERROR, "read data from [%s] fail, offset[%zu], len[%zu]",
                      this->_fileReader->DebugString().c_str(), dataBlockOffset, realDataLength);
            co_return indexlib::index::ConvertFSErrorCode(getHandlesRet[0].ec);
        }
        auto blockHandle = std::move(getHandlesRet[0].result);
        uint8_t* data = blockHandle.GetData();
        uint64_t inBlockOffset = accessor->GetInBlockOffset(dataBlockOffset);
        KVItem* dataStart = (KVItem*)(data + inBlockOffset);
        KVItem* dataEnd = dataStart + keyCountInBlock;
        co_return this->LocateItem(dataStart, dataEnd, key, value);
    }
    // cross over 2 cache blocks, will copy data
    uint64_t bufferSize = this->_dataBlockSize;
    std::vector<char> dataBuf(bufferSize);

    auto buf = dataBuf.data();
    indexlib::file_system::BatchIO batchIO;
    batchIO.emplace_back(buf, realDataLength, dataBlockOffset);
    auto readResult = co_await this->_fileReader->BatchRead(batchIO, option);
    assert(readResult.size() == 1);
    if (!readResult[0].OK()) {
        AUTIL_LOG(ERROR, "read data from [%s] fail, offset[%zu], len[%zu]", this->_fileReader->DebugString().c_str(),
                  dataBlockOffset, realDataLength);
        co_return indexlib::index::ConvertFSErrorCode(readResult[0].ec);
    }
    KVItem* dataStart = (KVItem*)dataBuf.data();
    KVItem* dataEnd = dataStart + keyCountInBlock;
    co_return this->LocateItem(dataStart, dataEnd, key, value);
}
}} // namespace indexlibv2::index
