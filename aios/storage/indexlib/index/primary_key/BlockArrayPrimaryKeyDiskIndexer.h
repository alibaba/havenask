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
#include "indexlib/index/primary_key/PrimaryKeyDiskIndexerTyped.h"

namespace indexlibv2::index {

template <typename Key>
class BlockArrayPrimaryKeyDiskIndexer : public PrimaryKeyDiskIndexerTyped<Key>
{
public:
    BlockArrayPrimaryKeyDiskIndexer() : _bloomFilter(nullptr) {}
    ~BlockArrayPrimaryKeyDiskIndexer() {}

public:
    bool Open(const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig,
              const std::shared_ptr<indexlib::file_system::IDirectory>& dir, const std::string fileName,
              const indexlib::file_system::FSOpenType openType) override;

    future_lite::coro::Lazy<indexlib::index::Result<docid_t>> LookupAsync(const Key& hashKey,
                                                                          future_lite::Executor* executor) noexcept
    {
        if (_bloomFilter && !_bloomFilter->Contains(hashKey)) {
            co_return INVALID_DOCID;
        }

        indexlib::file_system::ReadOption readOption;
        readOption.executor = executor;
        readOption.useInternalExecutor = false;
        docid_t docid = INVALID_DOCID;
        auto ret = co_await _blockArrayReader.FindAsync(hashKey, readOption, &docid);
        if (!ret.Ok()) {
            co_return ret.GetErrorCode();
        }
        if (ret.Value()) {
            co_return docid;
        } else {
            co_return INVALID_DOCID;
        }
    }

    indexlib::index::Result<docid_t> Lookup(const Key& hashKey) noexcept
    {
        if (_bloomFilter && !_bloomFilter->Contains(hashKey)) {
            return INVALID_DOCID;
        }

        docid_t docid = INVALID_DOCID;
        indexlib::file_system::ReadOption readOption;
        auto ret = _blockArrayReader.Find(hashKey, readOption, &docid);
        if (!ret.Ok()) {
            return ret.GetErrorCode();
        }
        if (ret.Value()) {
            return docid;
        } else {
            return INVALID_DOCID;
        }
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
    indexlibv2::index::BlockArrayReader<Key, docid_t> _blockArrayReader;
    autil::BloomFilter* _bloomFilter;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, BlockArrayPrimaryKeyDiskIndexer, T);

template <typename Key>
bool BlockArrayPrimaryKeyDiskIndexer<Key>::Open(
    const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig,
    const std::shared_ptr<indexlib::file_system::IDirectory>& directory, const std::string fileName,
    const indexlib::file_system::FSOpenType openType)
{
    assert(indexConfig->GetPrimaryKeyIndexType() == pk_block_array);
    auto [status, fileReader] = directory->CreateFileReader(fileName, openType).StatusWith();
    this->_fileReader = fileReader;
    if (!status.IsOK() || !this->_fileReader) {
        AUTIL_LOG(ERROR, "failed to create blockArray primaryKeyReader!");
        return false;
    }
    indexlib::file_system::FSResult<autil::BloomFilter*> bloomFilter =
        this->CreateBloomFilterReader(indexConfig, directory);
    if (bloomFilter.ec == indexlib::file_system::FSEC_OK) {
        _bloomFilter = bloomFilter.result;
    } else {
        return false;
    }

    try {
        _blockArrayReader.Init(this->_fileReader, indexlib::file_system::IDirectory::ToLegacyDirectory(directory),
                               this->_fileReader->GetLength(), true);
    } catch (...) {
        AUTIL_LOG(ERROR, "failed to init blockArrayReader!");
        return false;
    }
    return true;
}

} // namespace indexlibv2::index
