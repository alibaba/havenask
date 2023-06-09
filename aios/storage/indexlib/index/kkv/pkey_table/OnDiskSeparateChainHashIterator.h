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

#include "indexlib/index/common/hash_table/HashTableReader.h"
#include "indexlib/index/kkv/common/OnDiskPKeyOffset.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"

namespace indexlibv2::index {

class OnDiskSeparateChainHashIterator
{
public:
    OnDiskSeparateChainHashIterator();
    ~OnDiskSeparateChainHashIterator();

public:
    void Open(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& config,
              const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory);
    bool IsValid() const;
    void MoveToNext();
    void SortByKey();
    size_t Size() const;
    uint64_t Key() const;
    OnDiskPKeyOffset Value() const;
    void Reset()
    {
        _cursor = 0;
        ReadCurrentKeyValue();
    }

private:
    void ReadCurrentKeyValue();

private:
    using HashTable = HashTableReader<uint64_t, OnDiskPKeyOffset>;
    using HashNode = HashTableNode<uint64_t, OnDiskPKeyOffset>;

private:
    indexlib::file_system::FileReaderPtr _fileReader;
    uint32_t _size;
    uint32_t _cursor;
    uint64_t _key;
    OnDiskPKeyOffset _value;
};

} // namespace indexlibv2::index
