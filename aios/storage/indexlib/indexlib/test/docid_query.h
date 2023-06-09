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
#ifndef __INDEXLIB_DOCID_QUERY_H
#define __INDEXLIB_DOCID_QUERY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/query.h"

namespace indexlib { namespace test {

class DocidQuery : public Query
{
public:
    DocidQuery();
    ~DocidQuery();

public:
    bool Init(const std::string& docidStr);
    virtual docid_t Seek(docid_t docid);
    virtual pos_t SeekPosition(pos_t pos) { return INVALID_POSITION; }

private:
    std::vector<docid_t> mDocIdVec;
    size_t mCursor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocidQuery);
}} // namespace indexlib::test

#endif //__INDEXLIB_DOCID_QUERY_H
