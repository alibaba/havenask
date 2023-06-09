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
class HashDictionaryIteratorTyped : public DictionaryIterator
{
private:
    using HashItem = HashDictionaryItemTyped<KeyType>;

public:
    HashDictionaryIteratorTyped(HashItem* dictItemData, uint32_t dictItemCount);
    HashDictionaryIteratorTyped(HashItem* dictItemData, uint32_t dictItemCount, dictvalue_t nullTermValue);
    ~HashDictionaryIteratorTyped() = default;

    bool HasNext() const override;
    void Next(index::DictKeyInfo& key, dictvalue_t& value) override;
    bool IsValid() const { return _dataCursor < _dictItemCount; }
    void GetCurrent(dictkey_t& key, dictvalue_t& value)
    {
        key = _dictItemData[_dataCursor].key;
        value = _dictItemData[_dataCursor].value;
    }

    void MoveToNext() { _dataCursor++; }

private:
    void DoNext(KeyType& key, dictvalue_t& value);

    HashItem* _dictItemData;
    uint32_t _dictItemCount;
    uint32_t _dataCursor;
    dictvalue_t _nullTermValue;
    bool _hasNullTerm;

    AUTIL_LOG_DECLARE();
};

using HashDictionaryIterator = HashDictionaryIteratorTyped<dictkey_t>;
////////////////////////////////////////////
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, HashDictionaryIteratorTyped, KeyType);

template <typename KeyType>
HashDictionaryIteratorTyped<KeyType>::HashDictionaryIteratorTyped(HashItem* dictItemData, uint32_t dictItemCount)
    : _dictItemData(dictItemData)
    , _dictItemCount(dictItemCount)
    , _dataCursor(0)
    , _nullTermValue(0)
    , _hasNullTerm(false)
{
}

template <typename KeyType>
HashDictionaryIteratorTyped<KeyType>::HashDictionaryIteratorTyped(HashItem* dictItemData, uint32_t dictItemCount,
                                                                  dictvalue_t nullTermValue)
    : _dictItemData(dictItemData)
    , _dictItemCount(dictItemCount)
    , _dataCursor(0)
    , _nullTermValue(nullTermValue)
    , _hasNullTerm(true)
{
}

template <typename KeyType>
inline bool HashDictionaryIteratorTyped<KeyType>::HasNext() const
{
    return _dataCursor < _dictItemCount || _hasNullTerm;
}

template <typename KeyType>
inline void HashDictionaryIteratorTyped<KeyType>::Next(index::DictKeyInfo& key, dictvalue_t& value)
{
    if (_dataCursor < _dictItemCount) {
        KeyType typedKey;
        DoNext(typedKey, value);
        key = index::DictKeyInfo(typedKey, false);
    } else if (_hasNullTerm) {
        key = index::DictKeyInfo::NULL_TERM;
        value = _nullTermValue;
        _hasNullTerm = false;
    }
}

template <typename KeyType>
inline void HashDictionaryIteratorTyped<KeyType>::DoNext(KeyType& key, dictvalue_t& value)
{
    assert(HasNext());
    key = _dictItemData[_dataCursor].key;
    value = _dictItemData[_dataCursor].value;
    _dataCursor++;
}

} // namespace indexlib::index
