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

#include "indexlib/index/inverted_index/format/dictionary/DictionaryIterator.h"
#include "indexlib/index/inverted_index/format/dictionary/UtilDefine.h"

namespace indexlib::index {

template <typename KeyType>
class TieredDictionaryIteratorTyped : public DictionaryIterator
{
private:
    typedef DictionaryItemTyped<KeyType> DictionaryItem;
    class DictionaryCompare
    {
    public:
        bool operator()(const DictionaryItem& left, const KeyType& rightKey) { return left.first < rightKey; }

        bool operator()(const KeyType& leftKey, const DictionaryItem& right) { return leftKey < right.first; }
    };

public:
    TieredDictionaryIteratorTyped(KeyType* blockIndex, uint32_t blockCount, DictionaryItem* _dictItemData,
                                  uint32_t dictItemCount);

    TieredDictionaryIteratorTyped(KeyType* blockIndex, uint32_t blockCount, DictionaryItem* _dictItemData,
                                  uint32_t dictItemCount, dictvalue_t nullTermValue);

    ~TieredDictionaryIteratorTyped();

    bool HasNext() const override;
    void Next(index::DictKeyInfo& key, dictvalue_t& value) override;
    void Seek(dictkey_t key) override;
    future_lite::coro::Lazy<index::ErrorCode> SeekAsync(dictkey_t key, file_system::ReadOption option) noexcept override
    {
        Seek(key);
        co_return index::ErrorCode::OK;
    }
    future_lite::coro::Lazy<index::ErrorCode> NextAsync(index::DictKeyInfo& key, file_system::ReadOption option,
                                                        dictvalue_t& value) noexcept override
    {
        Next(key, value);
        co_return index::ErrorCode::OK;
    }

private:
    DictionaryItem* _dictItemData;
    KeyType* _blockIndex;
    uint32_t _dictItemCount;
    uint32_t _blockCount;
    uint32_t _dataCursor;
    dictvalue_t _nullTermValue;
    bool _hasNullTerm;
    bool _isDone;

    AUTIL_LOG_DECLARE();
};

using TieredDictionaryIterator = TieredDictionaryIteratorTyped<dictkey_t>;
////////////////////////////////////////////
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, TieredDictionaryIteratorTyped, KeyType);

template <typename KeyType>
TieredDictionaryIteratorTyped<KeyType>::TieredDictionaryIteratorTyped(KeyType* blockIndex, uint32_t blockCount,
                                                                      DictionaryItem* dictItemData,
                                                                      uint32_t dictItemCount)
    : _dictItemData(dictItemData)
    , _blockIndex(blockIndex)
    , _dictItemCount(dictItemCount)
    , _blockCount(blockCount)
    , _dataCursor(0)
    , _nullTermValue(0)
    , _hasNullTerm(false)
    , _isDone(false)

{
    if (_dictItemCount == 0) {
        _isDone = true;
    }
}

template <typename KeyType>
TieredDictionaryIteratorTyped<KeyType>::TieredDictionaryIteratorTyped(KeyType* blockIndex, uint32_t blockCount,
                                                                      DictionaryItem* dictItemData,
                                                                      uint32_t dictItemCount, dictvalue_t nullTermValue)
    : _dictItemData(dictItemData)
    , _blockIndex(blockIndex)
    , _dictItemCount(dictItemCount)
    , _blockCount(blockCount)
    , _dataCursor(0)
    , _nullTermValue(nullTermValue)
    , _hasNullTerm(true)
    , _isDone(false)
{
}

template <typename KeyType>
TieredDictionaryIteratorTyped<KeyType>::~TieredDictionaryIteratorTyped()
{
}

template <typename KeyType>
inline bool TieredDictionaryIteratorTyped<KeyType>::HasNext() const
{
    return !_isDone;
}

template <typename KeyType>
inline void TieredDictionaryIteratorTyped<KeyType>::Next(index::DictKeyInfo& key, dictvalue_t& value)
{
    assert(!_isDone);
    if (_dataCursor < _dictItemCount) {
        KeyType typedKey;
        typedKey = _dictItemData[_dataCursor].first;
        value = _dictItemData[_dataCursor].second;
        key = index::DictKeyInfo(typedKey, false);
        _dataCursor++;
        if (_dataCursor == _dictItemCount && !_hasNullTerm) {
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
void TieredDictionaryIteratorTyped<KeyType>::Seek(dictkey_t key)
{
    _isDone = false;
    KeyType* data = std::lower_bound(_blockIndex, _blockIndex + _blockCount, key);
    if (data < _blockIndex + _blockCount) {
        size_t blockCursor = data - _blockIndex;
        DictionaryItem* blockStart = _dictItemData + blockCursor * ITEM_COUNT_PER_BLOCK;
        DictionaryItem* blockEnd = blockStart + ITEM_COUNT_PER_BLOCK;
        if (blockCursor == _blockCount - 1) {
            blockEnd = _dictItemData + _dictItemCount;
        }
        DictionaryCompare comp;
        DictionaryItem* dictItem = std::lower_bound(blockStart, blockEnd, key, comp);
        if (dictItem == blockEnd) {
            assert(false); // no such case, first lower-bound hit means second lower-bound also hits
            return;
        }
        _dataCursor = dictItem - _dictItemData;
        return;
    }
    _isDone = true;
}
} // namespace indexlib::index
