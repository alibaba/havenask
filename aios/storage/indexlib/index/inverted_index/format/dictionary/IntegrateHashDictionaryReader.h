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
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"
#include "indexlib/index/inverted_index/format/dictionary/HashDictionaryIterator.h"
#include "indexlib/util/Exception.h"

namespace indexlib::index {

template <typename KeyType>
class IntegrateHashDictionaryReaderTyped : public DictionaryReader
{
public:
    using HashDictionaryIteratorPtr = std::shared_ptr<HashDictionaryIteratorTyped<KeyType>>;
    using HashItem = HashDictionaryItemTyped<KeyType>;

    IntegrateHashDictionaryReaderTyped();
    ~IntegrateHashDictionaryReaderTyped();

    void Open(const std::shared_ptr<file_system::FileReader>& fileReader, uint32_t blockCount, size_t blockDataOffset,
              dictvalue_t nullTermValue, bool hasNullTerm);

    Status Open(const std::shared_ptr<file_system::Directory>& directory, const std::string& fileName,
                bool supportFileCompress) override;

    index::Result<bool> InnerLookup(dictkey_t key, file_system::ReadOption option,
                                    dictvalue_t& value) noexcept override;

    std::shared_ptr<DictionaryIterator> CreateIterator() const override
    {
        uint32_t dictItemCount = ((uint8_t*)_blockIndex - (uint8_t*)_data) / sizeof(HashItem);

        if (_hasNullTerm) {
            return HashDictionaryIteratorPtr(
                new HashDictionaryIteratorTyped<KeyType>(_data, dictItemCount, _nullTermValue));
        }
        return HashDictionaryIteratorPtr(new HashDictionaryIteratorTyped<KeyType>(_data, dictItemCount));
    }

protected:
    std::shared_ptr<file_system::FileReader> _fileReader;
    HashItem* _data;

private:
    using DictionaryItem = DictionaryItemTyped<KeyType>;

    class DictionaryCompare
    {
    public:
        bool operator()(const DictionaryItem& left, const KeyType& rightKey) { return left.first < rightKey; }

        bool operator()(const KeyType& leftKey, const DictionaryItem& right) { return leftKey < right.first; }
    };

    uint32_t* _blockIndex;
    uint32_t _blockCount;

    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, IntegrateHashDictionaryReaderTyped, KeyType);

template <typename KeyType>
IntegrateHashDictionaryReaderTyped<KeyType>::IntegrateHashDictionaryReaderTyped()
    : _data(nullptr)
    , _blockIndex(nullptr)
    , _blockCount(0)
{
}

template <typename KeyType>
IntegrateHashDictionaryReaderTyped<KeyType>::~IntegrateHashDictionaryReaderTyped()
{
}

template <typename KeyType>
void IntegrateHashDictionaryReaderTyped<KeyType>::Open(const std::shared_ptr<file_system::FileReader>& fileReader,
                                                       uint32_t blockCount, size_t blockDataOffset,
                                                       dictvalue_t nullTermValue, bool hasNullTerm)
{
    assert(fileReader);
    _fileReader = fileReader;
    _data = (HashItem*)_fileReader->GetBaseAddress();
    _blockCount = blockCount;
    _blockIndex = (uint32_t*)((uint8_t*)_data + blockDataOffset);
    _nullTermValue = nullTermValue;
    _hasNullTerm = hasNullTerm;
}

template <typename KeyType>
Status IntegrateHashDictionaryReaderTyped<KeyType>::Open(const std::shared_ptr<file_system::Directory>& directory,
                                                         const std::string& fileName, bool supportFileCompress)
{
    assert(!supportFileCompress);
    std::shared_ptr<file_system::FileReader> fileReader =
        directory->CreateFileReader(fileName, file_system::FSOT_MEM_ACCESS);

    uint32_t blockCount = 0;
    size_t dictDataLen = 0;
    auto status = ReadBlockMeta<KeyType>(fileReader, sizeof(uint32_t), blockCount, dictDataLen);
    RETURN_IF_STATUS_ERROR(status, "read block meta fail");
    Open(fileReader, blockCount, dictDataLen, _nullTermValue, _hasNullTerm);
    return Status::OK();
}

template <typename KeyType>
index::Result<bool> IntegrateHashDictionaryReaderTyped<KeyType>::InnerLookup(dictkey_t key,
                                                                             file_system::ReadOption option,
                                                                             dictvalue_t& value) noexcept
{
    uint32_t blockIdx = key % _blockCount;
    uint32_t cursor = _blockIndex[blockIdx];
    while (cursor != std::numeric_limits<uint32_t>::max()) {
        const HashItem& item = _data[cursor];
        if (item.key == (KeyType)key) {
            value = item.value;
            return true;
        }

        // key sorted in file
        if (item.key < (KeyType)key) {
            return false;
        }
        cursor = item.next;
    }
    return false;
}

} // namespace indexlib::index
