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
#include "indexlib/test/docid_query.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, DocidQuery);

DocidQuery::DocidQuery() : Query(qt_docid), mCursor(0) {}

DocidQuery::~DocidQuery() {}

bool DocidQuery::Init(const std::string& docidStr)
{
    vector<docid_t> docidRange;
    StringUtil::fromString(docidStr, docidRange, "-");
    if (docidRange.size() == 1) {
        mDocIdVec.push_back(docidRange[0]);
        return true;
    }
    if (docidRange.size() == 2) {
        // [begin, end]
        for (docid_t docid = docidRange[0]; docid <= docidRange[1]; ++docid) {
            mDocIdVec.push_back(docid);
        }
        return true;
    }
    return false;
}

docid_t DocidQuery::Seek(docid_t docid)
{
    while (mCursor < mDocIdVec.size()) {
        docid_t retDocId = mDocIdVec[mCursor++];
        if (retDocId >= docid) {
            return retDocId;
        }
    }
    return INVALID_DOCID;
}
}} // namespace indexlib::test
