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
#ifndef __INDEXLIB_ATOMIC_QUERY_H
#define __INDEXLIB_ATOMIC_QUERY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/query.h"

namespace indexlib { namespace test {

class AtomicQuery : public Query
{
public:
    AtomicQuery();
    ~AtomicQuery();

public:
    bool Init(index::PostingIterator* iter, std::string indexName)
    {
        mIndexName = indexName;
        mIter = iter;
        return true;
    }

    virtual docid_t Seek(docid_t docid);
    virtual pos_t SeekPosition(pos_t pos);
    void SetWord(const std::string& word) { mWord = word; }
    std::string GetWord() { return mWord; }

    std::string GetIndexName() const { return mIndexName; }

private:
    index::PostingIterator* mIter;
    std::string mIndexName;
    std::string mWord;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AtomicQuery);
}} // namespace indexlib::test

#endif //__INDEXLIB_ATOMIC_QUERY_H
