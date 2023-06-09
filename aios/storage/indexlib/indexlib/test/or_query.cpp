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
#include "indexlib/test/or_query.h"

using namespace std;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, OrQuery);

OrQuery::OrQuery() : Query(qt_or)
{
    mInited = false;
    mCursor = 0;
}

OrQuery::~OrQuery() {}

docid_t OrQuery::Seek(docid_t docid)
{
    if (!mInited) {
        Init();
        mInited = true;
    }

    if (mCursor < mDocIds.size()) {
        return mDocIds[mCursor++];
    }
    return INVALID_DOCID;
}

void OrQuery::Init()
{
    set<docid_t> docIds;
    for (size_t i = 0; i < mQuerys.size(); ++i) {
        docid_t docId = INVALID_DOCID;
        do {
            docId = mQuerys[i]->Seek(docId);
            if (docId != INVALID_DOCID) {
                docIds.insert(docId);
            }
        } while (docId != INVALID_DOCID);
    }
    set<docid_t>::iterator it = docIds.begin();
    for (; it != docIds.end(); ++it) {
        mDocIds.push_back(*it);
    }
}

pos_t OrQuery::SeekPosition(pos_t pos) { return INVALID_POSITION; }
}} // namespace indexlib::test
