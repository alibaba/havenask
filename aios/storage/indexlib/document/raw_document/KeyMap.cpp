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
#include "indexlib/document/raw_document/KeyMap.h"

#include <algorithm>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

namespace indexlibv2 { namespace document {
AUTIL_LOG_SETUP(indexlib.document, KeyMap);

const size_t KeyMap::INVALID_INDEX = (size_t)-1;

KeyMap::KeyMap(size_t keyInitSize) : _hashMap(keyInitSize), _pool(new Pool(1024)), _keyInitSize(keyInitSize) {}

KeyMap::~KeyMap()
{
    _hashMap.clear();
    _keyFields.clear();
    DELETE_AND_SET_NULL(_pool);
}

KeyMap::KeyMap(const KeyMap& other)
    : _hashMap(other._keyInitSize)
    , _pool(new Pool(1024))
    , _keyInitSize(other._keyInitSize)
{
    _keyFields.reserve(other._keyFields.size());
    for (size_t i = 0; i < other._keyFields.size(); ++i) {
        StringView key = autil::MakeCString(other._keyFields[i], _pool);
        _hashMap.insert(make_pair(key, i));
        _keyFields.emplace_back(key);
    }
}

KeyMap* KeyMap::clone() const { return new KeyMap(*this); }

void KeyMap::merge(const KeyMap& increment)
{
    for (KeyFields::const_iterator it = increment._keyFields.begin(); it != increment._keyFields.end(); ++it) {
        findOrInsert(*it);
    }
}

size_t KeyMap::insert(const StringView& key)
{
    size_t indexNew = _hashMap.size();
    StringView newKey = autil::MakeCString(key, _pool);
    _keyFields.emplace_back(newKey);
    _hashMap[newKey] = indexNew;
    assert(indexNew + 1 == _hashMap.size());
    return indexNew;
}
}} // namespace indexlibv2::document
