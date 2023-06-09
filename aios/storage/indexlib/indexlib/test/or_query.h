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
#ifndef __INDEXLIB_OR_QUERY_H
#define __INDEXLIB_OR_QUERY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/query.h"

namespace indexlib { namespace test {

class OrQuery : public Query
{
public:
    OrQuery();
    ~OrQuery();

public:
    virtual docid_t Seek(docid_t docid);

    // TODO: not support
    virtual pos_t SeekPosition(pos_t pos);

    void AddQuery(QueryPtr query) { mQuerys.push_back(query); }
    size_t GetQueryCount() { return mQuerys.size(); }

private:
    void Init();

private:
    std::vector<QueryPtr> mQuerys;
    std::vector<docid_t> mDocIds;
    bool mInited;
    size_t mCursor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OrQuery);
}} // namespace indexlib::test

#endif //__INDEXLIB_OR_QUERY_H
