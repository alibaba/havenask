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
#include <unordered_map>

#include "autil/BytellHashmap.h"
#include "autil/ConstString.h"
#include "autil/StringView.h"
#include "autil/mem_pool/Pool.h"

namespace indexlibv2 { namespace document {

class KeyMap
{
private:
    struct StringHasher {
        size_t operator()(const autil::StringView& str) const
        {
            return std::_Hash_bytes(str.data(), str.size(), static_cast<size_t>(0xc70f6907UL));
        }
    };

    typedef autil::bytell_hash_map<autil::StringView, size_t, StringHasher> HashMap;
    typedef std::vector<autil::StringView> KeyFields;

public:
    KeyMap(size_t keyInitSize = 4096);
    virtual ~KeyMap();

private:
    KeyMap(const KeyMap&);
    KeyMap& operator=(const KeyMap&);

public:
    KeyMap* clone() const;
    void merge(const KeyMap&);
    size_t findOrInsert(const autil::StringView&);
    size_t find(const autil::StringView&) const;
    size_t insert(const autil::StringView&);
    const KeyFields& getKeyFields() const;
    virtual size_t size() const;

public:
    static const size_t INVALID_INDEX;

private:
    HashMap _hashMap;
    KeyFields _keyFields;
    autil::mem_pool::Pool* _pool;
    size_t _keyInitSize;

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////

inline size_t KeyMap::findOrInsert(const autil::StringView& key)
{
    HashMap::const_iterator it = _hashMap.find(key);
    if (it != _hashMap.end()) {
        return it->second;
    }
    return insert(key);
}

inline size_t KeyMap::find(const autil::StringView& key) const
{
    HashMap::const_iterator it = _hashMap.find(key);
    if (it != _hashMap.end()) {
        return it->second;
    }
    return INVALID_INDEX;
}

inline const KeyMap::KeyFields& KeyMap::getKeyFields() const { return _keyFields; }

inline size_t KeyMap::size() const { return _keyFields.size(); }
}} // namespace indexlibv2::document
