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

#include "autil/Log.h"
#include "indexlib/file_system/Directory.h"
namespace indexlibv2 { namespace index {

// BlockArrayIterator support iterator which read data on disk in sequence
template <typename Key, typename Value>
class BlockArrayIterator
{
public:
    BlockArrayIterator();
    ~BlockArrayIterator() {}

    BlockArrayIterator(const BlockArrayIterator&) = default;
    BlockArrayIterator& operator=(const BlockArrayIterator&) = default;

public:
    Status Init(const indexlib::file_system::FileReaderPtr& fileReader, int32_t blockSize, int32_t keyCount);
    bool HasNext() const;
    Status Next(Key* key, Value* value);
    void GetCurrentKVPair(Key* key, Value* value) const;
    size_t GetKeyCount() const { return _keyCount; }

private:
    indexlib::file_system::FileReaderPtr _fileReader;
    int32_t _blockSize;
    size_t _keyCount;

    int32_t _curOffsetInBlock;
    size_t _cursor;
    size_t _leftKeyCount;
    bool _isDone;
    Key _currentKey;
    Value _currentValue;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_2(indexlib.index, BlockArrayIterator, Key, Value);

template <typename Key, typename Value>
BlockArrayIterator<Key, Value>::BlockArrayIterator()
    : _blockSize(0)
    , _keyCount(0)
    , _curOffsetInBlock(0)
    , _cursor(0)
    , _leftKeyCount(0)
    , _isDone(false)
{
}

template <typename Key, typename Value>
Status BlockArrayIterator<Key, Value>::Init(const indexlib::file_system::FileReaderPtr& fileReader, int32_t blockSize,
                                            int32_t keyCount)
{
    _fileReader = fileReader;
    _blockSize = blockSize;
    _keyCount = keyCount;
    _curOffsetInBlock = 0;
    _cursor = 0;
    _leftKeyCount = keyCount;
    _isDone = false;
    return Next(&_currentKey, &_currentValue);
}

template <typename Key, typename Value>
bool BlockArrayIterator<Key, Value>::HasNext() const
{
    return !_isDone;
}

template <typename Key, typename Value>
Status BlockArrayIterator<Key, Value>::Next(Key* key, Value* value)
{
    assert(HasNext());
    GetCurrentKVPair(key, value);
    if (_leftKeyCount == 0) {
        _isDone = true;
        return Status::OK();
    }

    auto pairRet = _fileReader->Read((void*)(&_currentKey), sizeof(Key), _cursor).StatusWith();
    RETURN_IF_STATUS_ERROR(pairRet.first, "file[%s] read fail", _fileReader->GetLogicalPath().c_str());
    auto readLen = pairRet.second;
    pairRet = _fileReader->Read((void*)(&_currentValue), sizeof(Value), _cursor + readLen).StatusWith();
    RETURN_IF_STATUS_ERROR(pairRet.first, "file[%s] read fail", _fileReader->GetLogicalPath().c_str());
    readLen += pairRet.second;
    if (readLen != sizeof(Key) + sizeof(Value)) {
        RETURN_IF_STATUS_ERROR(Status::Corruption(), "read block array kv data file[%s] fail, file collapse!",
                               _fileReader->DebugString().c_str());
    }
    _cursor += readLen;
    _curOffsetInBlock += sizeof(Key) + sizeof(Value);
    _leftKeyCount--;
    // |---------------------------------|    Block Size
    // |---------|---------|---------|xxx|    left read < sizeof (key + value)
    if (_curOffsetInBlock + sizeof(Key) + sizeof(Value) > _blockSize) {
        _cursor += _blockSize - _curOffsetInBlock;
        _curOffsetInBlock = 0;
    }
    return Status::OK();
}

template <typename Key, typename Value>
void BlockArrayIterator<Key, Value>::GetCurrentKVPair(Key* key, Value* value) const
{
    *key = _currentKey;
    *value = _currentValue;
}
}} // namespace indexlibv2::index
