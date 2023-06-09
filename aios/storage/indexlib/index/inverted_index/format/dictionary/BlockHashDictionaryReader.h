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

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/BlockFileNode.h"
#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/index/inverted_index/format/dictionary/CommonDiskHashDictionaryIterator.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"
#include "indexlib/index/inverted_index/format/dictionary/UtilDefine.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib::index {

template <typename KeyType>
class BlockHashDictionaryReaderTyped : public DictionaryReader
{
public:
    typedef std::shared_ptr<DictionaryIterator> DictionaryIteratorPtr;
    typedef std::shared_ptr<CommonDiskHashDictionaryIteratorTyped<KeyType>> OnDiskDictionaryIteratorPtr;
    using HashItem = HashDictionaryItemTyped<KeyType>;

    BlockHashDictionaryReaderTyped();
    ~BlockHashDictionaryReaderTyped();

    void Open(const std::shared_ptr<file_system::Directory>& directory,
              const std::shared_ptr<file_system::FileReader>& fileReader, uint32_t blockCount, size_t blockIndexOffset,
              dictvalue_t nullTermValue, bool hasNullTerm);

    Status Open(const std::shared_ptr<file_system::Directory>& directory, const std::string& fileName,
                bool supportFileCompress) override;

    index::Result<bool> InnerLookup(dictkey_t key, file_system::ReadOption option,
                                    dictvalue_t& value) noexcept override;
    future_lite::coro::Lazy<index::Result<DictionaryReader::LookupResult>>
    InnerLookupAsync(dictkey_t key, file_system::ReadOption option) noexcept override
    {
        co_return co_await DoLookupAsync(key, option);
    }

    std::shared_ptr<DictionaryIterator> CreateIterator() const override;

private:
    future_lite::coro::Lazy<index::Result<LookupResult>> DoLookupAsync(KeyType key,
                                                                       file_system::ReadOption option) noexcept;

    std::shared_ptr<file_system::Directory> _directory;
    std::shared_ptr<file_system::FileReader> _fileReader;
    std::shared_ptr<file_system::BlockFileNode> _blockFileNode;
    uint32_t _blockCount;
    size_t _blockIndexOffset;

    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, BlockHashDictionaryReaderTyped, KeyType);

template <typename KeyType>
BlockHashDictionaryReaderTyped<KeyType>::BlockHashDictionaryReaderTyped() : _blockCount(0)
                                                                          , _blockIndexOffset(0)
{
}

template <typename KeyType>
BlockHashDictionaryReaderTyped<KeyType>::~BlockHashDictionaryReaderTyped()
{
}

template <typename KeyType>
void BlockHashDictionaryReaderTyped<KeyType>::Open(const std::shared_ptr<file_system::Directory>& directory,
                                                   const std::shared_ptr<file_system::FileReader>& fileReader,
                                                   uint32_t blockCount, size_t blockIndexOffset,
                                                   dictvalue_t nullTermValue, bool hasNullTerm)
{
    assert(fileReader);
    _directory = directory;
    _fileReader = fileReader;
    _blockCount = blockCount;
    _blockIndexOffset = blockIndexOffset;
    _nullTermValue = nullTermValue;
    _hasNullTerm = hasNullTerm;

    _blockFileNode = std::dynamic_pointer_cast<file_system::BlockFileNode>(fileReader->GetFileNode());
    if (!_blockFileNode) {
        INDEXLIB_FATAL_ERROR(UnSupported, "open [%s] with wrong open type [%d], should be FSOT_CACHE",
                             fileReader->DebugString().c_str(), fileReader->GetOpenType());
    }
}

template <typename KeyType>
Status BlockHashDictionaryReaderTyped<KeyType>::Open(const std::shared_ptr<file_system::Directory>& directory,
                                                     const std::string& fileName, bool supportFileCompress)
{
    std::shared_ptr<file_system::FileReader> fileReader =
        directory->CreateFileReader(fileName, file_system::FSOT_CACHE);
    uint32_t blockCount = 0;
    size_t dictDataLen = 0;
    auto status = ReadBlockMeta<KeyType>(fileReader, sizeof(uint32_t), blockCount, dictDataLen);
    RETURN_IF_STATUS_ERROR(status, "read block meta fail");
    assert(fileReader->GetBaseAddress() == nullptr);
    Open(directory, fileReader, blockCount, dictDataLen, _nullTermValue, _hasNullTerm);
    return Status::OK();
}

template <typename KeyType>
index::Result<bool> BlockHashDictionaryReaderTyped<KeyType>::InnerLookup(dictkey_t key, file_system::ReadOption option,
                                                                         dictvalue_t& value) noexcept
{
    auto resultWithEc = future_lite::coro::syncAwait(DoLookupAsync((KeyType)key, option));
    if (!resultWithEc.Ok()) {
        return resultWithEc.GetErrorCode();
    }
    auto result = resultWithEc.Value();
    if (result.first) {
        value = result.second;
    }
    return result.first;
}

template <typename KeyType>
inline std::shared_ptr<DictionaryIterator> BlockHashDictionaryReaderTyped<KeyType>::CreateIterator() const
{
    std::string fileName;
    util::PathUtil::GetRelativePath(_directory->GetLogicalPath(), _fileReader->GetLogicalPath(), fileName);
    std::shared_ptr<file_system::FileReader> fileReader =
        _directory->CreateFileReader(fileName, file_system::FSOT_BUFFERED);
    assert(fileReader);

    auto bufferFileReader = std::dynamic_pointer_cast<file_system::BufferedFileReader>(fileReader);
    assert(bufferFileReader);

    OnDiskDictionaryIteratorPtr iterator;
    if (_hasNullTerm) {
        iterator.reset(
            new CommonDiskHashDictionaryIteratorTyped<KeyType>(bufferFileReader, _blockIndexOffset, _nullTermValue));
    } else {
        iterator.reset(new CommonDiskHashDictionaryIteratorTyped<KeyType>(bufferFileReader, _blockIndexOffset));
    }
    return iterator;
}

template <typename KeyType>
inline future_lite::coro::Lazy<index::Result<DictionaryReader::LookupResult>>
BlockHashDictionaryReaderTyped<KeyType>::DoLookupAsync(KeyType key, file_system::ReadOption option) noexcept
{
    uint32_t blockIdx = key % _blockCount;
    uint32_t cursor = 0;
    size_t cursorOffset = blockIdx * sizeof(uint32_t) + _blockIndexOffset;
    file_system::BatchIO batchIO;
    batchIO.emplace_back(&cursor, sizeof(uint32_t), cursorOffset);
    auto ioResult = co_await _blockFileNode->BatchReadOrdered(batchIO, option);
    assert(1 == ioResult.size());
    if (!ioResult[0].OK()) {
        AUTIL_LOG(ERROR, "read block fail from file [%s], offset [%lu]", _blockFileNode->DebugString().c_str(),
                  cursorOffset);
        co_return index::ConvertFSErrorCode(ioResult[0].Code());
    }
    while (cursor != std::numeric_limits<uint32_t>::max()) {
        HashItem item;
        file_system::BatchIO itemBatchIO;
        itemBatchIO.emplace_back(&item, sizeof(item), cursor * sizeof(HashItem));
        auto itemIoResult = co_await _blockFileNode->BatchReadOrdered(itemBatchIO, option);
        assert(1 == itemIoResult.size());
        if (!itemIoResult[0].OK()) {
            AUTIL_LOG(ERROR, "read HashItem fail from file [%s], offset [%lu]", _blockFileNode->DebugString().c_str(),
                      cursor * sizeof(HashItem));
            co_return index::ConvertFSErrorCode(itemIoResult[0].Code());
        }

        if (item.key == (KeyType)key) {
            co_return std::make_pair(true, dictvalue_t(item.value));
        }
        // key sorted in file
        if (item.key < (KeyType)key) {
            co_return std::make_pair(false, dictvalue_t());
        }
        cursor = item.next;
    }
    co_return std::make_pair(false, dictvalue_t());
}

} // namespace indexlib::index
