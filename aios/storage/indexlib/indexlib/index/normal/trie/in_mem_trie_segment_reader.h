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
#include <map>
#include <memory>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/IndexSegmentReader.h"
#include "indexlib/index/normal/trie/double_array_trie.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class InMemTrieSegmentReader : public index::IndexSegmentReader
{
public:
    typedef std::map<autil::StringView, docid_t, std::less<autil::StringView>,
                     autil::mem_pool::pool_allocator<std::pair<const autil::StringView, docid_t>>>
        KVMap;

public:
    InMemTrieSegmentReader(autil::ReadWriteLock* dataLock, KVMap* kvMap);
    ~InMemTrieSegmentReader();

public:
    docid_t Lookup(const autil::StringView& pkStr) const;
    // TODO: support, if need in the future
    // bool PrefixSearch(const autil::StringView& key, KVPairVector& results,
    //                   int32_t maxMatches) const;
private:
    mutable autil::ReadWriteLock* mDataLock;
    KVMap* mKVMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemTrieSegmentReader);
//////////////////////////////////////////////////////////////////////////
inline docid_t InMemTrieSegmentReader::Lookup(const autil::StringView& pkStr) const
{
    autil::ScopedReadWriteLock lock(*mDataLock, 'R');
    auto iter = mKVMap->find(pkStr);
    if (iter == mKVMap->cend()) {
        return INVALID_DOCID;
    }
    return iter->second;
}
}} // namespace indexlib::index
