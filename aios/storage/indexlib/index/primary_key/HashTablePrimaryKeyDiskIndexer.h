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

#include "autil/Log.h"
#include "indexlib/index/primary_key/PrimaryKeyHashTable.h"

namespace indexlibv2::index {

template <typename Key>
class HashTablePrimaryKeyDiskIndexer : public PrimaryKeyDiskIndexerTyped<Key>
{
public:
    HashTablePrimaryKeyDiskIndexer() : _data(nullptr) {}
    ~HashTablePrimaryKeyDiskIndexer() {}

public:
    bool Open(const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig,
              const std::shared_ptr<indexlib::file_system::IDirectory>& dir, const std::string fileName,
              const indexlib::file_system::FSOpenType openType) override;
    indexlib::index::Result<docid_t> Lookup(const Key& hashKey) noexcept __ALWAYS_INLINE
    {
        return _pkHashTable.Find(hashKey);
    }

private:
    void* _data;
    PrimaryKeyHashTable<Key> _pkHashTable;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, HashTablePrimaryKeyDiskIndexer, T);

template <typename Key>
bool HashTablePrimaryKeyDiskIndexer<Key>::Open(
    const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig,
    const std::shared_ptr<indexlib::file_system::IDirectory>& dir, const std::string fileName,
    const indexlib::file_system::FSOpenType openType)
{
    if (openType != indexlib::file_system::FSOT_MEM_ACCESS && openType != indexlib::file_system::FSOT_SLICE) {
        AUTIL_LOG(ERROR, "HashTablePrimaryKeyDiskIndexer only support open with FSOT_MEM_ACCESS or FSOT_SLICE");
        return false;
    }
    auto [status, fileReader] = dir->CreateFileReader(fileName, openType).StatusWith();
    this->_fileReader = fileReader;
    if (!status.IsOK() || !this->_fileReader) {
        AUTIL_LOG(ERROR, "failed to create hashTable primaryKeyReader!");
        return false;
    }
    _data = this->_fileReader->GetBaseAddress();
    _pkHashTable.Init((char*)_data);
    return true;
}

} // namespace indexlibv2::index
