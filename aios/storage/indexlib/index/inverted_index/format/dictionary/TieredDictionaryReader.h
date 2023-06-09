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

#include "autil/BloomFilter.h"
#include "fslib/fslib.h"
#include "indexlib/file_system/file/ResourceFile.h"
#include "indexlib/index/inverted_index/format/dictionary/LegacyBlockArrayReader.h"

namespace indexlib::index {

template <typename KeyType>
class TieredDictionaryReaderTyped : public DictionaryReader
{
public:
    typedef std::shared_ptr<DictionaryIterator> DictionaryIteratorPtr;
    typedef std::shared_ptr<TieredDictionaryIteratorTyped<KeyType>> TieredDictionaryIteratorPtr;

    using LegacyBlockArrayReaderPtr = std::shared_ptr<LegacyBlockArrayReader<KeyType, dictvalue_t>>;

    TieredDictionaryReaderTyped()
        : _bloomFilterMultipleNum(0)
        , _bloomFilterHashFuncNum(0)
        , _bloomFilter(nullptr)
        , _disableSliceMemLock(autil::EnvUtil::getEnv<bool>("INDEXLIB_DISABLE_SLICE_MEM_LOCK", false))
    {
    }
    ~TieredDictionaryReaderTyped() {}

    void EnableBloomFilter(uint32_t multipleNum, uint32_t hashFuncNum) override
    {
        _bloomFilterMultipleNum = multipleNum;
        _bloomFilterHashFuncNum = hashFuncNum;
    }

    Status Open(const std::shared_ptr<file_system::Directory>& directory, const std::string& fileName,
                bool supportFileCompress) override;

    std::shared_ptr<DictionaryIterator> CreateIterator() const override;
    size_t EstimateMemUsed() const override;

    static size_t GetPartialLockSize(size_t fileLength) { return fileLength / (ITEM_COUNT_PER_BLOCK * 2); }
    DictionaryIterType GetIteratorType() const;

protected:
    void OpenBloomFilter(const std::shared_ptr<file_system::Directory>& directory);
    index::Result<bool> InnerLookup(dictkey_t key, file_system::ReadOption option,
                                    dictvalue_t& value) noexcept override;

    future_lite::coro::Lazy<index::Result<DictionaryReader::LookupResult>>
    InnerLookupAsync(dictkey_t key, file_system::ReadOption option) noexcept override;

private:
    LegacyBlockArrayReaderPtr _reader;
    uint32_t _bloomFilterMultipleNum;
    uint32_t _bloomFilterHashFuncNum;
    std::shared_ptr<file_system::ResourceFile> _resourceFile;
    autil::BloomFilter* _bloomFilter = nullptr;
    std::shared_ptr<file_system::Directory> _indexDir;
    std::string _dictFileName;
    bool _disableSliceMemLock = false;

    AUTIL_LOG_DECLARE();
};

typedef TieredDictionaryReaderTyped<dictkey_t> TieredDictionaryReader;
typedef std::shared_ptr<TieredDictionaryReader> TieredDictionaryReaderPtr;

//////////////////////////////////////////////////////////////////
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, TieredDictionaryReaderTyped, KeyType);

template <typename KeyType>
Status TieredDictionaryReaderTyped<KeyType>::Open(const std::shared_ptr<file_system::Directory>& directory,
                                                  const std::string& fileName, bool supportFileCompress)
{
    file_system::ReaderOption option(file_system::FSOT_LOAD_CONFIG);
    option.supportCompress = supportFileCompress;
    std::shared_ptr<file_system::FileReader> fileReader = directory->CreateFileReader(fileName, option);
    uint32_t blockCount = 0;
    size_t dictDataLen = 0;
    auto statusRead = ReadBlockMeta<KeyType>(fileReader, sizeof(KeyType), blockCount, dictDataLen);
    RETURN_IF_STATUS_ERROR(statusRead, "read block meta fail");
    LegacyBlockArrayReaderPtr reader;
    reader.reset(new LegacyBlockArrayReader<KeyType, dictvalue_t>(blockCount));
    auto [status, ret] = reader->Init(fileReader, directory, dictDataLen + sizeof(KeyType) * blockCount, true);
    RETURN_IF_STATUS_ERROR(status, "block array reader init fail");
    if (!ret) {
        RETURN_IF_STATUS_ERROR(Status::InternalError(),
                               "init LegacyBlockArrayReader block array failed"
                               " with fileReader[%s], open type[%d]",
                               fileReader->DebugString().c_str(), fileReader->GetOpenType());
    }
    _reader = reader;
    if (!fileReader->IsMemLock() && _bloomFilterMultipleNum > 0 && _bloomFilterHashFuncNum > 0) {
        OpenBloomFilter(directory);
    }
    _indexDir = directory;
    _dictFileName = fileName;
    return Status::OK();
}

template <typename KeyType>
inline void
TieredDictionaryReaderTyped<KeyType>::OpenBloomFilter(const std::shared_ptr<file_system::Directory>& directory)
{
    std::string bloomFilterFileName = "dictionary_bloom_filter";
    _resourceFile = directory->GetResourceFile(bloomFilterFileName);
    if (!_resourceFile || _resourceFile->Empty()) {
        std::shared_ptr<file_system::ResourceFile> writeResource = directory->CreateResourceFile(bloomFilterFileName);
        size_t bitSize = (size_t)_reader->GetItemCount() != 0
                             ? (size_t)_reader->GetItemCount() * _bloomFilterMultipleNum
                             : _bloomFilterMultipleNum;
        if (bitSize > std::numeric_limits<uint32_t>::max()) {
            bitSize = std::numeric_limits<uint32_t>::max();
        }
        std::unique_ptr<autil::BloomFilter> bloomFilter(
            new autil::BloomFilter((uint32_t)bitSize, _bloomFilterHashFuncNum));
        AUTIL_LOG(INFO, "Begin create bloomFilter File [%s], bitSize [%lu], multipleNum [%u], hashFuncNum [%u]",
                  writeResource->GetLogicalPath().c_str(), bitSize, _bloomFilterMultipleNum, _bloomFilterHashFuncNum);
        auto iter = _reader->CreateIteratorForDictionary(_hasNullTerm, _nullTermValue, /*disableCache*/ true);
        if (iter != nullptr) {
            index::DictKeyInfo key;
            dictvalue_t value;
            while (iter->HasNext()) {
                iter->Next(key, value);
                if (!key.IsNull()) {
                    bloomFilter->Insert(key.GetKey());
                }
            }
        }
        writeResource->UpdateMemoryUse(bitSize / 8);
        writeResource->Reset(bloomFilter.release());
        _resourceFile = writeResource;
        AUTIL_LOG(INFO, "Finish create bloomFilter File [%s], bitSize [%lu], multipleNum [%u], hashFuncNum [%u]",
                  writeResource->GetLogicalPath().c_str(), bitSize, _bloomFilterMultipleNum, _bloomFilterHashFuncNum);
    }
    _bloomFilter = _resourceFile->GetResource<autil::BloomFilter>();
}

template <typename KeyType>
inline index::Result<bool> TieredDictionaryReaderTyped<KeyType>::InnerLookup(dictkey_t key,
                                                                             file_system::ReadOption option,
                                                                             dictvalue_t& value) noexcept
{
    if (_bloomFilter != nullptr && !_bloomFilter->Contains(key)) {
        return false;
    }
    return _reader->Find(key, option, &value);
}

template <typename KeyType>
future_lite::coro::Lazy<index::Result<DictionaryReader::LookupResult>> inline TieredDictionaryReaderTyped<
    KeyType>::InnerLookupAsync(dictkey_t key, file_system::ReadOption option) noexcept
{
    dictvalue_t value;
    if (_bloomFilter != nullptr && !_bloomFilter->Contains(key)) {
        co_return DictionaryReader::LookupResult(false, 0);
    }
    auto findResult = co_await _reader->FindAsync(key, option, &value);
    if (!findResult.Ok()) {
        co_return findResult.GetErrorCode();
    }
    co_return std::make_pair(findResult.Value(), value);
}

template <typename KeyType>
DictionaryIterType TieredDictionaryReaderTyped<KeyType>::GetIteratorType() const
{
    return _reader->GetDictionaryIterType();
}

template <typename KeyType>
typename TieredDictionaryReaderTyped<KeyType>::DictionaryIteratorPtr
TieredDictionaryReaderTyped<KeyType>::CreateIterator() const
{
    return _reader->CreateIteratorForDictionary(_hasNullTerm, _nullTermValue, /*disableCache=*/false);
}

template <typename KeyType>
size_t TieredDictionaryReaderTyped<KeyType>::EstimateMemUsed() const
{
    size_t totalMemUse = 0;
    if (_indexDir && _indexDir->IsExist(_dictFileName)) {
        size_t memLockSize = _indexDir->EstimateFileMemoryUse(_dictFileName, file_system::FSOT_LOAD_CONFIG);
        if (memLockSize != 0) {
            totalMemUse += memLockSize;
        } else if (!_disableSliceMemLock ||
                   _indexDir->EstimateFileMemoryUse(_dictFileName, file_system::FSOT_MEM_ACCESS) > 0) {
            // enable slice memLock or block cache (ignore disable slice memLock && mem-nolock)
            size_t fileLength = _indexDir->GetFileLength(_dictFileName);
            totalMemUse += GetPartialLockSize(fileLength);
        }
    }
    if (_resourceFile) {
        totalMemUse += _resourceFile->GetLength();
    }
    return totalMemUse;
}

} // namespace indexlib::index
