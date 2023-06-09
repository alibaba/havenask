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
#ifndef __INDEXLIB_SOURCE_QUERY_H
#define __INDEXLIB_SOURCE_QUERY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/query.h"

namespace indexlib { namespace test {

class SourceQuery : public Query
{
public:
    SourceQuery();
    ~SourceQuery();

public:
    bool Init(index::PostingIterator* iter, std::string indexName, index::groupid_t groupId);
    index::groupid_t GetGroupId() const;
    virtual docid_t Seek(docid_t docid) override;
    virtual pos_t SeekPosition(pos_t pos) override;
    std::string GetIndexName() const;

private:
    index::PostingIterator* mIter;
    index::groupid_t mGroupId;
    std::string mIndexName;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SourceQuery);
}} // namespace indexlib::test

#endif //__INDEXLIB_SOURCE_QUERY_H
