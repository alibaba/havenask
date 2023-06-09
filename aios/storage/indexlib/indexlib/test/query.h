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
#ifndef __INDEXLIB_QUERY_H
#define __INDEXLIB_QUERY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace test {

enum QueryType {
    qt_atomic = 0,
    qt_and,
    qt_or,
    qt_docid,
    qt_source,
    qt_unknown,
};

class Query
{
public:
    Query(QueryType queryType);
    virtual ~Query();

public:
    virtual docid_t Seek(docid_t docid) = 0;
    virtual pos_t SeekPosition(pos_t pos) = 0;
    QueryType GetQueryType() const { return mQueryType; }

    void SetSubQuery() { mIsSubQuery = true; }
    bool IsSubQuery() const { return mIsSubQuery; }

private:
    bool mIsSubQuery;
    QueryType mQueryType;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(Query);
}} // namespace indexlib::test

#endif //__INDEXLIB_QUERY_H
