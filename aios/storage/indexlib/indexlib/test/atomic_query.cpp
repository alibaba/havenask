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
#include "indexlib/test/atomic_query.h"

using namespace std;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, AtomicQuery);

AtomicQuery::AtomicQuery() : Query(qt_atomic) { mIter = NULL; }

AtomicQuery::~AtomicQuery()
{
    if (mIter) {
        auto pool = mIter->GetSessionPool();
        IE_POOL_COMPATIBLE_DELETE_CLASS(pool, mIter);
        mIter = NULL;
    }
}

docid_t AtomicQuery::Seek(docid_t docid) { return mIter->SeekDoc(docid); }

pos_t AtomicQuery::SeekPosition(pos_t pos)
{
    if (!mIter->HasPosition()) {
        return INVALID_POSITION;
    }
    return mIter->SeekPosition(pos);
}
}} // namespace indexlib::test
