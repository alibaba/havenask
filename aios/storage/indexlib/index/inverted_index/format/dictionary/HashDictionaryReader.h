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

#include "fslib/fslib.h"
#include "indexlib/index/inverted_index/format/dictionary/BlockHashDictionaryReader.h"
#include "indexlib/index/inverted_index/format/dictionary/IntegrateHashDictionaryReader.h"

namespace indexlib::index {

template <typename KeyType>
class HashDictionaryReaderTyped : public DictionaryReader
{
public:
    using IntegrateDictReader = IntegrateHashDictionaryReaderTyped<KeyType>;
    using BlockDictReader = BlockHashDictionaryReaderTyped<KeyType>;

    HashDictionaryReaderTyped() = default;
    ~HashDictionaryReaderTyped() = default;

    Status Open(const std::shared_ptr<file_system::Directory>& directory, const std::string& fileName,
                bool supportFileCompress) override;
    size_t EstimateMemUsed() const override;

protected:
    index::Result<bool> InnerLookup(dictkey_t key, file_system::ReadOption option,
                                    dictvalue_t& value) noexcept override;
    inline future_lite::coro::Lazy<index::Result<DictionaryReader::LookupResult>>
    InnerLookupAsync(dictkey_t key, file_system::ReadOption option) noexcept override;

    std::shared_ptr<DictionaryIterator> CreateIterator() const override;

private:
    std::shared_ptr<DictionaryReader> _innerDictionaryReader;
    std::shared_ptr<file_system::Directory> _indexDir;
    std::string _dictFileName;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, HashDictionaryReaderTyped, KeyType);

using HashDictionaryReader = HashDictionaryReaderTyped<dictkey_t>;

template <typename KeyType>
Status HashDictionaryReaderTyped<KeyType>::Open(const std::shared_ptr<file_system::Directory>& directory,
                                                const std::string& fileName, bool supportFileCompress)
{
    std::shared_ptr<file_system::FileReader> fileReader =
        directory->CreateFileReader(fileName, file_system::FSOT_LOAD_CONFIG);
    uint32_t blockCount = 0;
    size_t dictDataLen = 0;
    auto status = ReadBlockMeta<KeyType>(fileReader, sizeof(uint32_t), blockCount, dictDataLen);
    RETURN_IF_STATUS_ERROR(status, "read block meta fail");
    _indexDir = directory;
    _dictFileName = fileName;
    if (fileReader->GetBaseAddress()) {
        auto reader = std::make_shared<IntegrateDictReader>();
        reader->Open(fileReader, blockCount, dictDataLen, _nullTermValue, _hasNullTerm);
        _innerDictionaryReader = reader;
        return Status::OK();
    }
    auto reader = std::make_shared<BlockDictReader>();
    reader->Open(directory, fileReader, blockCount, dictDataLen, _nullTermValue, _hasNullTerm);
    _innerDictionaryReader = reader;
    return Status::OK();
}

template <typename KeyType>
index::Result<bool> HashDictionaryReaderTyped<KeyType>::InnerLookup(dictkey_t key, file_system::ReadOption option,
                                                                    dictvalue_t& value) noexcept
{
    return _innerDictionaryReader->InnerLookup(key, option, value);
}

template <typename KeyType>
future_lite::coro::Lazy<index::Result<DictionaryReader::LookupResult>>
HashDictionaryReaderTyped<KeyType>::InnerLookupAsync(dictkey_t key, file_system::ReadOption option) noexcept
{
    co_return co_await _innerDictionaryReader->InnerLookupAsync(key, option);
}

template <typename KeyType>
typename HashDictionaryReaderTyped<KeyType>::DictionaryIteratorPtr
HashDictionaryReaderTyped<KeyType>::CreateIterator() const
{
    return _innerDictionaryReader->CreateIterator();
}

template <typename KeyType>
size_t HashDictionaryReaderTyped<KeyType>::EstimateMemUsed() const
{
    size_t totalMemUse = 0;
    if (_indexDir && _indexDir->IsExist(_dictFileName)) {
        totalMemUse += _indexDir->EstimateFileMemoryUse(_dictFileName, file_system::FSOT_LOAD_CONFIG);
    }
    return totalMemUse;
}

} // namespace indexlib::index
