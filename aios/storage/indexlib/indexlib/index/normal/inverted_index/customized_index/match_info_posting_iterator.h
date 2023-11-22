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
#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"
#include "indexlib/indexlib.h"

namespace indexlibv2::index {
class TermMeta;
}

namespace indexlib { namespace index {

class MatchInfoPostingIterator : public PostingIterator
{
public:
    MatchInfoPostingIterator(const std::vector<SegmentMatchInfo>& segMatchInfos, autil::mem_pool::Pool* sessionPool);
    ~MatchInfoPostingIterator();

private:
    MatchInfoPostingIterator(const MatchInfoPostingIterator& other);

public:
    TermMeta* GetTermMeta() const override;
    matchvalue_t GetMatchValue() const override { return mCurMatchValue; }
    MatchValueType GetMatchValueType() const override { return mType; }
    docid64_t SeekDoc(docid64_t docId) override;
    index::ErrorCode SeekDocWithErrorCode(docid64_t docId, docid64_t& result) override;
    bool HasPosition() const override;
    void Unpack(TermMatchData& termMatchData) override;
    PostingIteratorType GetType() const override { return pi_customized; }
    void Reset() override;
    PostingIterator* Clone() const override;
    autil::mem_pool::Pool* GetSessionPool() const override { return mSessionPool; }

private:
    bool LocateSegment(docid_t docId, int32_t& segCursor, docid_t*& docCursor, MatchValueType& type,
                       matchvalue_t*& matchValueCursor, docid_t& curSegBaseDocId, docid_t& lastDocIdInCurSeg);

private:
    std::vector<SegmentMatchInfo> mSegMatchInfos;
    autil::mem_pool::Pool* mSessionPool;
    TermMeta* mTermMeta;
    docid_t* mDocCursor;
    matchvalue_t* mMatchValueCursor;
    docid_t mCurSegBaseDocId;
    docid_t mCurrentDocId;
    MatchValueType mType;
    matchvalue_t mCurMatchValue;
    int32_t mSegCursor;
    docid_t mLastDocIdInCurSeg;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MatchInfoPostingIterator);

inline bool MatchInfoPostingIterator::LocateSegment(docid_t docId, int32_t& segCursor, docid_t*& docCursor,
                                                    MatchValueType& type, matchvalue_t*& matchValueCursor,
                                                    docid_t& curSegBaseDocId, docid_t& lastDocIdInCurSeg)
{
    int segCount = mSegMatchInfos.size();
    int segIdx = std::max(0, segCursor);
    for (; segIdx < segCount; ++segIdx) {
        docid_t lastDocIdInSeg = mSegMatchInfos[segIdx].GetLastMatchDocId();
        if (lastDocIdInSeg >= docId) {
            segCursor = segIdx;
            docCursor = mSegMatchInfos[segIdx].matchInfo->docIds;
            matchValueCursor = mSegMatchInfos[segIdx].matchInfo->matchValues;
            curSegBaseDocId = mSegMatchInfos[segIdx].baseDocId;
            lastDocIdInCurSeg = lastDocIdInSeg;
            return true;
        }
    }
    return false;
}

inline docid64_t MatchInfoPostingIterator::SeekDoc(docid64_t docId)
{
    docid64_t ret = INVALID_DOCID;
    auto ec = SeekDocWithErrorCode(docId, ret);
    assert(ec == index::ErrorCode::OK);
    (void)ec;
    return ret;
}

inline index::ErrorCode MatchInfoPostingIterator::SeekDocWithErrorCode(docid64_t docId, docid64_t& result)
{
    docId = std::max((docid64_t)mCurrentDocId + 1, docId);
    if (unlikely(docId > mLastDocIdInCurSeg)) {
        if (!LocateSegment(docId, mSegCursor, mDocCursor, mType, mMatchValueCursor, mCurSegBaseDocId,
                           mLastDocIdInCurSeg)) {
            result = INVALID_DOCID;
            return index::ErrorCode::OK;
        }
    }
    docid_t localDocId = docId - mCurSegBaseDocId;
    for (; *mDocCursor < localDocId; ++mDocCursor, ++mMatchValueCursor)
        ;

    mCurrentDocId = *mDocCursor + mCurSegBaseDocId;
    mCurMatchValue = *mMatchValueCursor;
    // mNeedMoveToCurrentDoc = true;
    result = mCurrentDocId;
    return index::ErrorCode::OK;
}
}} // namespace indexlib::index
