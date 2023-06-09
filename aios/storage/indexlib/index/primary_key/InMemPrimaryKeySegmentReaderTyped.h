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

#include "indexlib/base/Constant.h"
#include "indexlib/index/inverted_index/IndexSegmentReader.h"
#include "indexlib/util/HashMap.h"

namespace indexlib { namespace index {

class AttributeSegmentReader;

template <typename Key>
class InMemPrimaryKeySegmentReaderTyped : public IndexSegmentReader
{
public:
    typedef util::HashMap<Key, docid_t> HashMapTyped;
    typedef std::shared_ptr<HashMapTyped> HashMapTypedPtr;
    typedef typename util::HashMap<Key, docid_t>::KeyValuePair KeyValuePair;
    typedef typename HashMapTyped::Iterator Iterator;

public:
    InMemPrimaryKeySegmentReaderTyped(HashMapTypedPtr hashMap, std::shared_ptr<AttributeSegmentReader> pkAttrReader =
                                                                   std::shared_ptr<AttributeSegmentReader>());

public:
    docid_t Lookup(const Key& key) const { return _hashMap->Find(key, INVALID_DOCID); }

    std::shared_ptr<AttributeSegmentReader> GetPKAttributeReader() const override { return _pkAttrReader; }
    Iterator CreateIterator() const { return _hashMap->CreateIterator(); }
    std::shared_ptr<const HashMapTyped> GetHashMap() const { return _hashMap; }

private:
    HashMapTypedPtr _hashMap;
    std::shared_ptr<AttributeSegmentReader> _pkAttrReader;
};

template <typename Key>
InMemPrimaryKeySegmentReaderTyped<Key>::InMemPrimaryKeySegmentReaderTyped(
    HashMapTypedPtr hashMap, std ::shared_ptr<AttributeSegmentReader> pkAttrReader)
    : _hashMap(std::move(hashMap))
    , _pkAttrReader(std::move(pkAttrReader))
{
}

}} // namespace indexlib::index
