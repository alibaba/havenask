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
#include "indexlib/index/normal/inverted_index/customized_index/match_info_posting_iterator.h"

#include "indexlib/index/inverted_index/TermMatchData.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, MatchInfoPostingIterator);

MatchInfoPostingIterator::MatchInfoPostingIterator(const vector<SegmentMatchInfo>& segMatchInfos,
                                                   autil::mem_pool::Pool* sessionPool)
    : mSegMatchInfos(segMatchInfos)
    , mSessionPool(sessionPool)
    , mDocCursor(NULL)
    , mMatchValueCursor(NULL)
    , mCurSegBaseDocId(INVALID_DOCID)
    , mCurrentDocId(INVALID_DOCID)
    , mCurMatchValue(matchvalue_t())
    , mSegCursor(-1)
    , mLastDocIdInCurSeg(INVALID_DOCID)
{
    df_t docFreq = 0;
    for (const auto& segMatchInfo : segMatchInfos) {
        docFreq += segMatchInfo.matchInfo->matchCount;
    }
    mTermMeta = IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool, TermMeta, docFreq, docFreq, 0);
}

MatchInfoPostingIterator::~MatchInfoPostingIterator() { IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mTermMeta); }

TermMeta* MatchInfoPostingIterator::GetTermMeta() const { return mTermMeta; }

bool MatchInfoPostingIterator::HasPosition() const { return false; }

void MatchInfoPostingIterator::Unpack(TermMatchData& termMatchData) { termMatchData.SetMatched(true); }

void MatchInfoPostingIterator::Reset()
{
    mDocCursor = NULL;
    mMatchValueCursor = NULL;
    mCurSegBaseDocId = INVALID_DOCID;
    mCurrentDocId = INVALID_DOCID;
    mCurMatchValue = matchvalue_t();
    mSegCursor = -1;
    mLastDocIdInCurSeg = INVALID_DOCID;
}

PostingIterator* MatchInfoPostingIterator::Clone() const
{
    MatchInfoPostingIterator* iter =
        IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool, MatchInfoPostingIterator, mSegMatchInfos, mSessionPool);
    return iter;
}
}} // namespace indexlib::index
