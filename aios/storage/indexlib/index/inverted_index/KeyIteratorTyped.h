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
#include <queue>
#include <sstream>

#include "indexlib/index/inverted_index/KeyIterator.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryIterator.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"

namespace indexlib::index {

struct KeyInfo {
    std::shared_ptr<DictionaryIterator> dictIter;
    index::DictKeyInfo key;
};

class KeyComparator
{
public:
    bool operator()(KeyInfo*& lft, KeyInfo*& rht) { return lft->key > rht->key; }
};

template <class IndexReaderType>
class KeyIteratorTyped : public KeyIterator
{
public:
    KeyIteratorTyped(IndexReaderType& reader);
    ~KeyIteratorTyped();

public:
    bool HasNext() const override;
    PostingIterator* NextPosting(std::string& key) override;

private:
    void ClearKeyHeap();

private:
    void Init();

private:
    typedef std::priority_queue<KeyInfo*, std::vector<KeyInfo*>, KeyComparator> KeyHeap;

private:
    IndexReaderType& _reader;
    KeyHeap _keyHeap;

private:
    AUTIL_LOG_DECLARE();
};

/////////////////////////////////////////////

template <class IndexReaderType>
KeyIteratorTyped<IndexReaderType>::KeyIteratorTyped(IndexReaderType& reader) : KeyIterator()
                                                                             , _reader(reader)
{
    Init();
}

template <class IndexReaderType>
KeyIteratorTyped<IndexReaderType>::~KeyIteratorTyped()
{
    ClearKeyHeap();
}

template <class IndexReaderType>
void KeyIteratorTyped<IndexReaderType>::Init()
{
    const auto& dictReaders = _reader.GetDictReaders();
    for (size_t i = 0; i < dictReaders.size(); ++i) {
        KeyInfo* keyInfo = new KeyInfo;
        if (!dictReaders[i]) {
            delete keyInfo;
            continue;
        }
        keyInfo->dictIter = dictReaders[i]->CreateIterator();
        if (!keyInfo->dictIter->HasNext()) {
            delete keyInfo;
            continue;
        }
        dictvalue_t value;
        keyInfo->dictIter->Next(keyInfo->key, value);
        _keyHeap.push(keyInfo);
    }
}

template <class IndexReaderType>
void KeyIteratorTyped<IndexReaderType>::ClearKeyHeap()
{
    while (!_keyHeap.empty()) {
        KeyInfo* keyInfo = _keyHeap.top();
        delete keyInfo;
        _keyHeap.pop();
    }
}

template <class IndexReaderType>
bool KeyIteratorTyped<IndexReaderType>::HasNext() const
{
    return !_keyHeap.empty();
}

template <class IndexReaderType>
PostingIterator* KeyIteratorTyped<IndexReaderType>::NextPosting(std::string& strKey)
{
    index::DictKeyInfo key;
    KeyInfo* keyInfo = _keyHeap.top();
    _keyHeap.pop();

    key = keyInfo->key;
    if (!keyInfo->dictIter->HasNext()) {
        delete keyInfo;
    } else {
        dictvalue_t value;
        keyInfo->dictIter->Next(keyInfo->key, value);
        _keyHeap.push(keyInfo);
    }

    strKey = key.ToString();
    while (!_keyHeap.empty()) {
        keyInfo = _keyHeap.top();
        if (keyInfo->key != key) {
            break;
        }
        _keyHeap.pop();
        if (!keyInfo->dictIter->HasNext()) {
            delete keyInfo;
        } else {
            dictvalue_t value;
            keyInfo->dictIter->Next(keyInfo->key, value);
            _keyHeap.push(keyInfo);
        }
    }

    return future_lite::coro::syncAwait(
               _reader.CreateMainPostingIteratorAsync(key, /*statePoolSize*/ 1000, /*sessionPool*/ NULL,
                                                      /*needBuildingSegment*/ true, /*readOption*/ nullptr))
        .ValueOrThrow();
}
} // namespace indexlib::index
