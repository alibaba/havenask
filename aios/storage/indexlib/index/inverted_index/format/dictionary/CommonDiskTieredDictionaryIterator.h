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
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/file_system/stream/FileStreamCreator.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryIterator.h"
#include "indexlib/index/inverted_index/format/dictionary/UtilDefine.h"

namespace indexlib::index {

template <typename KeyType>
class CommonDiskTieredDictionaryIteratorTyped : public DictionaryIterator
{
public:
    using DictionaryItem = DictionaryItemTyped<KeyType>;
    class DictionaryCompare
    {
    public:
        bool operator()(const DictionaryItem& left, const KeyType& rightKey) { return left.first < rightKey; }

        bool operator()(const KeyType& leftKey, const DictionaryItem& right) { return leftKey < right.first; }
    };

    // fill @blockIndex if use @Seek
    CommonDiskTieredDictionaryIteratorTyped(std::shared_ptr<file_system::FileReader> fileReader, size_t dictDataLength,
                                            KeyType* blockIndex, uint32_t blockCount);
    CommonDiskTieredDictionaryIteratorTyped(std::shared_ptr<file_system::FileReader> fileReader, size_t dictDataLength,
                                            KeyType* blockIndex, uint32_t blockCount, dictvalue_t nullTermValue);
    ~CommonDiskTieredDictionaryIteratorTyped() = default;

    bool HasNext() const override;
    void Next(index::DictKeyInfo& key, dictvalue_t& value) override;
    void Seek(dictkey_t key) override;

    future_lite::coro::Lazy<index::ErrorCode> SeekAsync(dictkey_t key,
                                                        file_system::ReadOption option) noexcept override;
    future_lite::coro::Lazy<index::ErrorCode> NextAsync(index::DictKeyInfo& key, file_system::ReadOption option,
                                                        dictvalue_t& value) noexcept override;

private:
    std::shared_ptr<file_system::FileStream> _fileStream;
    size_t _dictDataLength;
    size_t _offset;
    dictvalue_t _nullTermValue;
    KeyType* _blockIndex;
    uint32_t _blockCount;
    bool _hasNullTerm;
    bool _isDone;

    AUTIL_LOG_DECLARE();
};

using CommonDiskTieredDictionaryIterator = CommonDiskTieredDictionaryIteratorTyped<dictkey_t>;

///////////////////////////////////////////////////////////////////////////
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, CommonDiskTieredDictionaryIteratorTyped, KeyType);

template <typename KeyType>
CommonDiskTieredDictionaryIteratorTyped<KeyType>::CommonDiskTieredDictionaryIteratorTyped(
    std::shared_ptr<file_system::FileReader> fileReader, size_t dictDataLength, KeyType* blockIndex,
    uint32_t blockCount)
    : _fileStream(file_system::FileStreamCreator::CreateFileStream(fileReader, nullptr))
    , _dictDataLength(dictDataLength)
    , _offset(0)
    , _nullTermValue(0)
    , _blockIndex(blockIndex)
    , _blockCount(blockCount)
    , _hasNullTerm(false)
    , _isDone(false)

{
    if (_dictDataLength == 0) {
        _isDone = true;
    }
}

template <typename KeyType>
CommonDiskTieredDictionaryIteratorTyped<KeyType>::CommonDiskTieredDictionaryIteratorTyped(
    std::shared_ptr<file_system::FileReader> fileReader, size_t dictDataLength, KeyType* blockIndex,
    uint32_t blockCount, dictvalue_t nullTermValue)
    : _fileStream(file_system::FileStreamCreator::CreateFileStream(fileReader, nullptr))
    , _dictDataLength(dictDataLength)
    , _offset(0)
    , _nullTermValue(nullTermValue)
    , _blockIndex(blockIndex)
    , _blockCount(blockCount)
    , _hasNullTerm(true)
    , _isDone(false)
{
}

template <typename KeyType>
bool CommonDiskTieredDictionaryIteratorTyped<KeyType>::HasNext() const
{
    return !_isDone;
}

template <typename KeyType>
void CommonDiskTieredDictionaryIteratorTyped<KeyType>::Next(index::DictKeyInfo& key, dictvalue_t& value)
{
    assert(!_isDone);
    if (_offset + 1 < _dictDataLength) {
        KeyType typedKey;
        size_t rKeyLen =
            _fileStream->Read((void*)&typedKey, sizeof(KeyType), _offset, file_system::ReadOption()).GetOrThrow();
        size_t rValueLen =
            _fileStream->Read((void*)&value, sizeof(dictvalue_t), _offset + sizeof(KeyType), file_system::ReadOption())
                .GetOrThrow();
        if (rKeyLen < sizeof(KeyType) || rValueLen < sizeof(dictvalue_t)) {
            INDEXLIB_FATAL_ERROR(FileIO, "read DictItem from file[%s] FAILED!", _fileStream->DebugString().c_str());
        }
        _offset += (rKeyLen + rValueLen);
        key = index::DictKeyInfo(typedKey, false);
        if (_offset == _dictDataLength && !_hasNullTerm) {
            _isDone = true;
        }
    } else {
        assert(_hasNullTerm);
        key = index::DictKeyInfo::NULL_TERM;
        value = _nullTermValue;
        _isDone = true;
    }
}

template <typename KeyType>
void CommonDiskTieredDictionaryIteratorTyped<KeyType>::Seek(dictkey_t key)
{
    // block index and data both use binary-search
    static_assert(sizeof(KeyType) + sizeof(dictvalue_t) == sizeof(DictionaryItem));
    if (unlikely(!_blockIndex)) {
        assert(false);
        _isDone = true;
        return;
    }

    size_t blockId = std::lower_bound(_blockIndex, _blockIndex + _blockCount, key) - _blockIndex;
    if (blockId == _blockCount) {
        _isDone = true;
        return;
    }
    _isDone = false;

    size_t blockSize = ITEM_COUNT_PER_BLOCK * sizeof(DictionaryItem);
    size_t readLength = std::min(blockSize, _dictDataLength - blockSize * blockId);
    std::vector<char> dataBuf(readLength);
    char* buffer = dataBuf.data();
    size_t readed = _fileStream->Read(buffer, readLength, blockId * blockSize, file_system::ReadOption()).GetOrThrow();
    if (readed != readLength) {
        INDEXLIB_FATAL_ERROR(FileIO, "read data from [%s] fail, offset[%zu], len[%zu], readed[%zu]",
                             _fileStream->DebugString().c_str(), blockId * blockSize, readLength, readed);
    }
    _offset += readed;

    DictionaryItem* start = (DictionaryItem*)buffer;
    DictionaryItem* end = (DictionaryItem*)(buffer + readLength);
    DictionaryCompare comp;
    size_t itemId = std::lower_bound(start, end, key, comp) - start + blockId * ITEM_COUNT_PER_BLOCK;
    assert(itemId != _dictDataLength / sizeof(DictionaryItem));
    _offset = itemId * sizeof(DictionaryItem);
    return;
}

template <typename KeyType>
inline future_lite::coro::Lazy<index::ErrorCode>
CommonDiskTieredDictionaryIteratorTyped<KeyType>::SeekAsync(dictkey_t key, file_system::ReadOption option) noexcept
{
    // block index and data both use binary-search
    static_assert(sizeof(KeyType) + sizeof(dictvalue_t) == sizeof(DictionaryItem));
    if (unlikely(!_blockIndex)) {
        assert(false);
        _isDone = true;
        co_return index::ErrorCode::OK;
    }

    size_t blockId = std::lower_bound(_blockIndex, _blockIndex + _blockCount, key) - _blockIndex;
    if (blockId == _blockCount) {
        _isDone = true;
        co_return index::ErrorCode::OK;
    }
    _isDone = false;

    size_t blockSize = ITEM_COUNT_PER_BLOCK * sizeof(DictionaryItem);
    size_t readLength = std::min(blockSize, _dictDataLength - blockSize * blockId);
    std::vector<char> dataBuf(readLength);
    char* buffer = dataBuf.data();
    file_system::BatchIO batchIO;
    batchIO.emplace_back(buffer, readLength, blockId * blockSize);
    auto ioResult = co_await _fileStream->BatchRead(batchIO, option);
    assert(1 == ioResult.size());
    if (!ioResult[0].OK()) {
        co_return index::ConvertFSErrorCode(ioResult[0].ec);
    }
    _offset += ioResult[0].result;
    DictionaryItem* start = (DictionaryItem*)buffer;
    DictionaryItem* end = (DictionaryItem*)(buffer + readLength);
    DictionaryCompare comp;
    size_t itemId = std::lower_bound(start, end, key, comp) - start + blockId * ITEM_COUNT_PER_BLOCK;
    assert(itemId != _dictDataLength / sizeof(DictionaryItem));
    _offset = itemId * sizeof(DictionaryItem);
    co_return index::ErrorCode::OK;
}
template <typename KeyType>
inline future_lite::coro::Lazy<index::ErrorCode>
CommonDiskTieredDictionaryIteratorTyped<KeyType>::NextAsync(index::DictKeyInfo& key, file_system::ReadOption option,
                                                            dictvalue_t& value) noexcept
{
    assert(!_isDone);
    if (_offset + 1 < _dictDataLength) {
        KeyType typedKey;
        file_system::BatchIO batchIO;
        batchIO.emplace_back((void*)&typedKey, sizeof(KeyType), _offset);
        batchIO.emplace_back((void*)&value, sizeof(dictvalue_t), _offset + sizeof(KeyType));
        auto ioResult = co_await _fileStream->BatchRead(batchIO, option);
        for (size_t i = 0; i < ioResult.size(); ++i) {
            if (ioResult[i].OK()) {
                _offset += ioResult[i].result;
            } else {
                co_return index::ConvertFSErrorCode(ioResult[i].ec);
            }
        }
        key = index::DictKeyInfo(typedKey, false);
        if (_offset == _dictDataLength && !_hasNullTerm) {
            _isDone = true;
        }
    } else {
        assert(_hasNullTerm);
        key = index::DictKeyInfo::NULL_TERM;
        value = _nullTermValue;
        _isDone = true;
    }
    co_return index::ErrorCode::OK;
}

} // namespace indexlib::index
