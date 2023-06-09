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

namespace indexlibv2::index {

template <typename Key>
class SortArrayPrimaryKeyDiskIndexer : public PrimaryKeyDiskIndexerTyped<Key>
{
public:
    SortArrayPrimaryKeyDiskIndexer() : _itemCount(0), _data(nullptr), _bloomFilter(nullptr) {}
    ~SortArrayPrimaryKeyDiskIndexer() {}
    using PKPairTyped = PKPair<Key, docid_t>;

public:
    bool Open(const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig,
              const std::shared_ptr<indexlib::file_system::IDirectory>& directory, const std::string fileName,
              const indexlib::file_system::FSOpenType openType) override;

    indexlib::index::Result<docid_t> Lookup(const Key& hashKey) noexcept
    {
        if (_bloomFilter && !_bloomFilter->Contains(hashKey)) {
            return INVALID_DOCID;
        }

        PKPairTyped* begin = (PKPairTyped*)_data;
        PKPairTyped* end = begin + _itemCount;
        PKPairTyped* iter = std::lower_bound(begin, end, hashKey);
        if (iter != end && iter->key == hashKey) {
            return iter->docid;
        }
        return INVALID_DOCID;
    }

    size_t EvaluateCurrentMemUsed() const override
    {
        size_t bloomFilterSize = 0;
        if (this->_bloomFilter) {
            bloomFilterSize = this->_bloomFilter->getBitsBufferSize();
        }
        return PrimaryKeyDiskIndexerTyped<Key>::EvaluateCurrentMemUsed() + bloomFilterSize;
    }

private:
    uint32_t _itemCount;
    void* _data;
    autil::BloomFilter* _bloomFilter;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SortArrayPrimaryKeyDiskIndexer, T);

template <typename Key>
bool SortArrayPrimaryKeyDiskIndexer<Key>::Open(
    const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig,
    const std::shared_ptr<indexlib::file_system::IDirectory>& directory, const std::string fileName,
    const indexlib::file_system::FSOpenType openType)
{
    if (openType != indexlib::file_system::FSOT_MEM_ACCESS) {
        AUTIL_LOG(ERROR, "sortArrayPrimaryKeyDiskIndexer only support open with FSOT_MEM_ACCESS");
        return false;
    }
    assert(indexConfig->GetPrimaryKeyIndexType() == pk_sort_array);
    auto [status, fileReader] =
        directory->CreateFileReader(fileName, indexlib::file_system::FSOT_MEM_ACCESS).StatusWith();
    this->_fileReader = fileReader;
    if (!status.IsOK() || !this->_fileReader) {
        AUTIL_LOG(ERROR, "failed to create sortArray primaryKeyReader!");
        return false;
    }

    indexlib::file_system::FSResult<autil::BloomFilter*> bloomFilter =
        this->CreateBloomFilterReader(indexConfig, directory);
    if (bloomFilter.ec == indexlib::file_system::FSEC_OK) {
        _bloomFilter = bloomFilter.result;
    } else {
        return false;
    }

    _itemCount = this->_fileReader->GetLogicLength() / sizeof(PKPairTyped);

    _data = this->_fileReader->GetBaseAddress();
    return true;
}

} // namespace indexlibv2::index
