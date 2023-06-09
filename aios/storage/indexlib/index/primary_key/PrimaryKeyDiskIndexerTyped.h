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

#include "autil/BloomFilter.h"
#include "autil/NoCopyable.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/NormalFileReader.h"
#include "indexlib/index/primary_key/BlockArrayPrimaryKeyLeafIterator.h"
#include "indexlib/index/primary_key/HashTablePrimaryKeyLeafIterator.h"
#include "indexlib/index/primary_key/PrimaryKeyLeafIterator.h"
#include "indexlib/index/primary_key/SortArrayPrimaryKeyLeafIterator.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"

namespace indexlibv2::index {

template <typename Key>
class PrimaryKeyDiskIndexerTyped : public autil::NoCopyable
{
public:
    PrimaryKeyDiskIndexerTyped() {}
    virtual ~PrimaryKeyDiskIndexerTyped() {}

public:
    using PKPairTyped = PKPair<Key, docid_t>;

public:
    virtual bool Open(const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig,
                      const std::shared_ptr<indexlib::file_system::IDirectory>& dir, const std::string fileName,
                      const indexlib::file_system::FSOpenType openType) = 0;
    static std::unique_ptr<PrimaryKeyLeafIterator<Key>>
    CreatePrimaryKeyLeafIterator(const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig,
                                 const indexlib::file_system::FileReaderPtr& reader) noexcept;

    static std::unique_ptr<PrimaryKeyLeafIterator<Key>>
    CreatePrimaryKeyLeafIterator(PrimaryKeyIndexType pkType,
                                 const indexlib::file_system::FileReaderPtr& reader) noexcept;

    const indexlib::file_system::FileReaderPtr& GetFileReader() const { return _fileReader; }

    virtual size_t EvaluateCurrentMemUsed() const
    {
        if (this->_fileReader->IsMemLock()) {
            return this->_fileReader->GetLength();
        }
        return 0;
    }

private:
    indexlib::file_system::FileReaderPtr
    GetFileReaderForLoadBloomFilter(const std::shared_ptr<indexlib::file_system::IDirectory>& directory);

protected:
    indexlib::file_system::FSResult<autil::BloomFilter*>
    CreateBloomFilterReader(const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig,
                            const std::shared_ptr<indexlib::file_system::IDirectory>& directory);

protected:
    std::shared_ptr<indexlib::file_system::FileReader> _fileReader;
    std::shared_ptr<indexlib::file_system::ResourceFile> _resourceFile;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, PrimaryKeyDiskIndexerTyped, T);

template <typename Key>
std::unique_ptr<PrimaryKeyLeafIterator<Key>> PrimaryKeyDiskIndexerTyped<Key>::CreatePrimaryKeyLeafIterator(
    const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig,
    const indexlib::file_system::FileReaderPtr& reader) noexcept
{
    return CreatePrimaryKeyLeafIterator(indexConfig->GetPrimaryKeyIndexType(), reader);
}

template <typename Key>
std::unique_ptr<PrimaryKeyLeafIterator<Key>> PrimaryKeyDiskIndexerTyped<Key>::CreatePrimaryKeyLeafIterator(
    PrimaryKeyIndexType pkType, const indexlib::file_system::FileReaderPtr& reader) noexcept
{
    std::unique_ptr<PrimaryKeyLeafIterator<Key>> iterator(nullptr);
    switch (pkType) {
    case pk_sort_array:
        iterator = std::make_unique<SortArrayPrimaryKeyLeafIterator<Key>>();
        break;
    case pk_block_array:
        iterator = std::make_unique<BlockArrayPrimaryKeyLeafIterator<Key>>();
        break;
    case pk_hash_table:
        iterator = std::make_unique<HashTablePrimaryKeyLeafIterator<Key>>();
        break;
    default:
        AUTIL_LOG(ERROR, "Not support unknown index type [%d] for iterator", pkType);
        return nullptr;
    }
    if (!iterator->Init(reader).IsOK()) {
        AUTIL_LOG(ERROR, "PrimaryKeyLeafIterator init failed");
        return nullptr;
    }
    return iterator;
}

template <typename Key>
indexlib::file_system::FileReaderPtr PrimaryKeyDiskIndexerTyped<Key>::GetFileReaderForLoadBloomFilter(
    const std::shared_ptr<indexlib::file_system::IDirectory>& directory)
{
    if (_fileReader->GetOpenType() == indexlib::file_system::FSOT_CACHE) {
        std::string relativePath;
        if (indexlib::util::PathUtil::GetRelativePath(directory->GetLogicalPath(), _fileReader->GetLogicalPath(),
                                                      relativePath)) {
            auto [status, fileReader] =
                directory
                    ->CreateFileReader(relativePath, indexlib::file_system::ReaderOption::NoCache(
                                                         indexlib::file_system::FSOT_BUFFERED))
                    .StatusWith();
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "create file reader fail.");
                return nullptr;
            }
            return fileReader;
        }
    }
    return indexlib::file_system::FileReaderPtr(
        new indexlib::file_system::NormalFileReader(_fileReader->GetFileNode()));
}

template <typename Key>
indexlib::file_system::FSResult<autil::BloomFilter*> PrimaryKeyDiskIndexerTyped<Key>::CreateBloomFilterReader(
    const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& indexConfig,
    const std::shared_ptr<indexlib::file_system::IDirectory>& directory)
{
    using namespace indexlib::file_system;
    const static std::string bloomFilterFileName = "pk_bloom_filter";
    auto itemCount = _fileReader->GetLogicLength() / sizeof(PKPairTyped);
    uint32_t multipleNum = 0;
    uint32_t hashFuncNum = 0;
    if (_fileReader->IsMemLock() || !indexConfig->GetBloomFilterParamForPkReader(multipleNum, hashFuncNum)) {
        return {FSEC_OK, nullptr};
    }
    assert(directory);
    auto [status, resourceFile] = directory->GetResourceFile(bloomFilterFileName).StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "get bloomfilter resource file fail.");
        return {FSEC_ERROR, nullptr};
    }
    if (!resourceFile || resourceFile->Empty()) {
        auto [newStatus, writeResource] = directory->CreateResourceFile(bloomFilterFileName).StatusWith();
        if (!newStatus.IsOK()) {
            AUTIL_LOG(ERROR, "create bloomfilter resource file fail.");
            return {FSEC_ERROR, nullptr};
        }

        size_t bitSize = itemCount != 0 ? (size_t)itemCount * multipleNum : multipleNum;
        if (bitSize > std::numeric_limits<uint32_t>::max()) {
            bitSize = std::numeric_limits<uint32_t>::max();
        }
        std::unique_ptr<autil::BloomFilter> bloomFilter(new autil::BloomFilter((uint32_t)bitSize, hashFuncNum));
        AUTIL_LOG(INFO, "Begin create bloomFilter File [%s], bitSize [%lu], multipleNum [%u], hashFuncNum [%u]",
                  writeResource->GetLogicalPath().c_str(), bitSize, multipleNum, hashFuncNum);
        indexlib::file_system::FileReaderPtr fileReader = GetFileReaderForLoadBloomFilter(directory);
        if (!fileReader) {
            AUTIL_LOG(ERROR, "Create file reader for loading bloomfilter failed.");
            return {FSEC_ERROR, nullptr};
        }
        std::unique_ptr<PrimaryKeyLeafIterator<Key>> pkPairIter = CreatePrimaryKeyLeafIterator(indexConfig, fileReader);
        if (!pkPairIter) {
            AUTIL_LOG(ERROR, "Create primary key iterator failed.");
            return {FSEC_ERROR, nullptr};
        }
        PKPairTyped pkPair;
        while (pkPairIter->HasNext()) {
            if (!pkPairIter->Next(pkPair).IsOK()) {
                AUTIL_LOG(ERROR, "primaryKeyIter do next failed!");
                return {FSEC_ERROR, nullptr};
            }
            bloomFilter->Insert(pkPair.key);
        }
        writeResource->UpdateMemoryUse(bitSize / 8);
        writeResource->Reset(bloomFilter.release());
        resourceFile = writeResource;
        AUTIL_LOG(INFO, "Finish create bloomFilter File [%s], bitSize [%lu], multipleNum [%u], hashFuncNum [%u]",
                  writeResource->GetLogicalPath().c_str(), bitSize, multipleNum, hashFuncNum);
    }
    _resourceFile = resourceFile;
    return {FSEC_OK, resourceFile->GetResource<autil::BloomFilter>()};
}

} // namespace indexlibv2::index
