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
#ifndef __INDEXLIB_TRIE_INDEX_ITERATOR_H
#define __INDEXLIB_TRIE_INDEX_ITERATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class TrieIndexIterator
{
public:
    TrieIndexIterator(DoubleArrayTrie::KVPairVector* results) : mResults(results), mCursor(0) { assert(mResults); }
    ~TrieIndexIterator()
    {
        // IE_POOL_COMPATIBLE_DELETE_CLASS(mResults->get_allocator()._pool, mResults);
    }

public:
    typedef std::pair<autil::StringView, docid_t> KVPair;
    typedef std::vector<KVPair, autil::mem_pool::pool_allocator<KVPair>> KVPairVector;

public:
    bool Valid() const { return mCursor < mResults->size(); }
    void Next()
    {
        if (Valid()) {
            mCursor++;
        }
    }
    const autil::StringView& GetKey() const
    {
        assert(Valid());
        return (*mResults)[mCursor].first;
    }
    docid_t GetDocid() const
    {
        assert(Valid());
        return (*mResults)[mCursor].second;
    }

public:
    // for test
    size_t Size() const { return mResults->size(); }

private:
    DoubleArrayTrie::KVPairVector* mResults;
    size_t mCursor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TrieIndexIterator);
}} // namespace indexlib::index

#endif //__INDEXLIB_TRIE_INDEX_ITERATOR_H
