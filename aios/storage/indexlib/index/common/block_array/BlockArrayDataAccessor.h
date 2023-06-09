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

#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/common/block_array/KeyValueItem.h"

namespace indexlibv2 { namespace index {
template <typename Key, typename Value>
class BlockArrayDataAccessor
{
public:
    using KVItem = indexlib::index::KeyValueItem<Key, Value>;
    enum AccessMode { MEMORY, CACHE, COMPRESS };

public:
    BlockArrayDataAccessor() : _dataBlockSize(0) {}
    virtual ~BlockArrayDataAccessor() {}

    BlockArrayDataAccessor(const BlockArrayDataAccessor&) = delete;
    BlockArrayDataAccessor& operator=(const BlockArrayDataAccessor&) = delete;
    BlockArrayDataAccessor(BlockArrayDataAccessor&&) = delete;
    BlockArrayDataAccessor& operator=(BlockArrayDataAccessor&&) = delete;

public:
    virtual Status Init(const indexlib::file_system::FileReaderPtr& fileReader, uint64_t dataBlockSize) = 0;
    virtual indexlib::index::Result<bool> GetValueInBlock(const Key& key, uint64_t blockId, uint64_t keyCountInBlock,
                                                          indexlib::file_system::ReadOption option,
                                                          Value* value) const noexcept
    {
        return future_lite::coro::syncAwait(GetValueInBlockAsync(key, blockId, keyCountInBlock, option, value));
    }
    virtual future_lite::coro::Lazy<indexlib::index::Result<bool>>
    GetValueInBlockAsync(const Key& key, uint64_t blockId, uint64_t keyCountInBlock,
                         indexlib::file_system::ReadOption option, Value* value) const noexcept = 0;
    virtual AccessMode GetMode() const = 0;

protected:
    inline bool LocateItem(KVItem* dataStart, KVItem* dataEnd, const Key& key, Value* value) const noexcept;

protected:
    indexlib::file_system::FileReaderPtr _fileReader;
    uint64_t _dataBlockSize;
};

template <typename Key, typename Value>
inline bool BlockArrayDataAccessor<Key, Value>::LocateItem(KVItem* dataStart, KVItem* dataEnd, const Key& key,
                                                           Value* value) const noexcept
{
    KVItem* kvItem = std::lower_bound(dataStart, dataEnd, key);
    if (kvItem == dataEnd || kvItem->key != key) {
        return false;
    }
    *value = kvItem->value;
    return true;
}
}} // namespace indexlibv2::index
