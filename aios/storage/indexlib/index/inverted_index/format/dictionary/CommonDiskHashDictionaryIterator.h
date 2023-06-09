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
#include "indexlib/index/inverted_index/format/dictionary/DictionaryIterator.h"
#include "indexlib/index/inverted_index/format/dictionary/UtilDefine.h"

namespace indexlib::index {

template <typename KeyType>
class CommonDiskHashDictionaryIteratorTyped : public DictionaryIterator
{
public:
    CommonDiskHashDictionaryIteratorTyped(std::shared_ptr<file_system::FileReader> fileReader, size_t dictDataLength);
    CommonDiskHashDictionaryIteratorTyped(std::shared_ptr<file_system::FileReader> fileReader, size_t dictDataLength,
                                          dictvalue_t nullTermValue);
    ~CommonDiskHashDictionaryIteratorTyped() = default;

    bool HasNext() const override;
    void Next(index::DictKeyInfo& key, dictvalue_t& value) override;

private:
    void DoNext(KeyType& key, dictvalue_t& value);

    file_system::FileReaderPtr _fileReader;
    size_t _dictDataLength;
    dictvalue_t _nullTermValue;
    bool _hasNullTerm;

    AUTIL_LOG_DECLARE();
};

using CommonDiskHashDictionaryIterator = CommonDiskHashDictionaryIteratorTyped<dictkey_t>;

///////////////////////////////////////////////////////////////////////////
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, CommonDiskHashDictionaryIteratorTyped, KeyType);

template <typename KeyType>
CommonDiskHashDictionaryIteratorTyped<KeyType>::CommonDiskHashDictionaryIteratorTyped(
    std::shared_ptr<file_system::FileReader> fileReader, size_t dictDataLength)
    : _fileReader(fileReader)
    , _dictDataLength(dictDataLength)
    , _nullTermValue(0)
    , _hasNullTerm(false)
{
    _fileReader->Seek(0).GetOrThrow();
}

template <typename KeyType>
CommonDiskHashDictionaryIteratorTyped<KeyType>::CommonDiskHashDictionaryIteratorTyped(
    std::shared_ptr<file_system::FileReader> fileReader, size_t dictDataLength, dictvalue_t nullTermValue)
    : _fileReader(fileReader)
    , _dictDataLength(dictDataLength)
    , _nullTermValue(nullTermValue)
    , _hasNullTerm(true)
{
    _fileReader->Seek(0).GetOrThrow();
}

template <typename KeyType>
bool CommonDiskHashDictionaryIteratorTyped<KeyType>::HasNext() const
{
    return ((size_t)_fileReader->Tell() + 1 < _dictDataLength) || _hasNullTerm;
}

template <typename KeyType>
void CommonDiskHashDictionaryIteratorTyped<KeyType>::Next(index::DictKeyInfo& key, dictvalue_t& value)
{
    if ((size_t)_fileReader->Tell() + 1 < _dictDataLength) {
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
void CommonDiskHashDictionaryIteratorTyped<KeyType>::DoNext(KeyType& key, dictvalue_t& value)
{
    HashDictionaryItemTyped<KeyType> item;
    size_t len = _fileReader->Read((void*)&item, sizeof(item)).GetOrThrow();
    if (len < sizeof(item)) {
        INDEXLIB_FATAL_ERROR(FileIO, "read DictItem from file[%s] FAILED!", _fileReader->DebugString().c_str());
    }

    key = item.key;
    value = item.value;
}
} // namespace indexlib::index
