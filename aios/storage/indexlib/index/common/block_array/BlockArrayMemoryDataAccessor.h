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

#include "indexlib/index/common/block_array/BlockArrayDataAccessor.h"

namespace indexlibv2 { namespace index {

template <typename Key, typename Value>
class BlockArrayMemoryDataAccessor : public BlockArrayDataAccessor<Key, Value>
{
public:
    using KVItem = typename BlockArrayDataAccessor<Key, Value>::KVItem;
    using AccessMode = typename BlockArrayDataAccessor<Key, Value>::AccessMode;
    BlockArrayMemoryDataAccessor() : _data(nullptr) {}
    virtual ~BlockArrayMemoryDataAccessor() {}

    BlockArrayMemoryDataAccessor(const BlockArrayMemoryDataAccessor&) = delete;
    BlockArrayMemoryDataAccessor& operator=(const BlockArrayMemoryDataAccessor&) = delete;
    BlockArrayMemoryDataAccessor(BlockArrayMemoryDataAccessor&&) = delete;
    BlockArrayMemoryDataAccessor& operator=(BlockArrayMemoryDataAccessor&&) = delete;

public:
    Status Init(const indexlib::file_system::FileReaderPtr& fileReader, uint64_t dataBlockSize) override;
    inline indexlib::index::Result<bool> GetValueInBlock(const Key& key, uint64_t blockId, uint64_t keyCountInBlock,
                                                         indexlib::file_system::ReadOption option,
                                                         Value* value) const noexcept override;

    inline future_lite::coro::Lazy<indexlib::index::Result<bool>>
    GetValueInBlockAsync(const Key& key, uint64_t blockId, uint64_t keyCountInBlock,
                         indexlib::file_system::ReadOption option, Value* value) const noexcept override;
    AccessMode GetMode() const override { return AccessMode::MEMORY; }

private:
    char* _data;
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP_TEMPLATE_2(indexlib.index, BlockArrayMemoryDataAccessor, Key, Value);

template <typename Key, typename Value>
Status BlockArrayMemoryDataAccessor<Key, Value>::Init(const indexlib::file_system::FileReaderPtr& fileReader,
                                                      uint64_t dataBlockSize)
{
    this->_fileReader = fileReader;
    this->_dataBlockSize = dataBlockSize;
    assert(this->_fileReader->GetBaseAddress());
    _data = (char*)this->_fileReader->GetBaseAddress();
    if (!_data) {
        RETURN_IF_STATUS_ERROR(Status::InternalError(), "base address is nullptr");
    }
    return Status::OK();
}

template <typename Key, typename Value>
inline indexlib::index::Result<bool>
BlockArrayMemoryDataAccessor<Key, Value>::GetValueInBlock(const Key& key, uint64_t blockId, uint64_t keyCountInBlock,
                                                          indexlib::file_system::ReadOption option,
                                                          Value* value) const noexcept
{
    uint64_t dataBlockOffset = this->_dataBlockSize * blockId;
    KVItem* dataStart = (KVItem*)((char*)_data + dataBlockOffset);
    KVItem* dataEnd = dataStart + keyCountInBlock;
    return this->LocateItem(dataStart, dataEnd, key, value);
}

template <typename Key, typename Value>
inline future_lite::coro::Lazy<indexlib::index::Result<bool>>
BlockArrayMemoryDataAccessor<Key, Value>::GetValueInBlockAsync(const Key& key, uint64_t blockId,
                                                               uint64_t keyCountInBlock,
                                                               indexlib::file_system::ReadOption option,
                                                               Value* value) const noexcept
{
    co_return this->GetValueInBlock(key, blockId, keyCountInBlock, option, value);
}
}} // namespace indexlibv2::index
