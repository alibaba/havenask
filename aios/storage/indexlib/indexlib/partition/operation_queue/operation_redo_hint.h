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
#ifndef __INDEXLIB_OPERATION_REDO_HINT_H
#define __INDEXLIB_OPERATION_REDO_HINT_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace partition {

class OperationRedoHint
{
public:
    enum HintType { REDO_USE_HINT_DOC, REDO_CACHE_SEGMENT, REDO_DOC_RANGE, REDO_UNKNOWN };

public:
    OperationRedoHint() { Reset(); }
    ~OperationRedoHint() {}

public:
    bool IsValid() const
    {
        return (mSegId != INVALID_SEGMENTID && mDocId != INVALID_DOCID && mHintType == REDO_USE_HINT_DOC) ||
               mHintType == REDO_CACHE_SEGMENT || mHintType == REDO_DOC_RANGE;
    }

    void Reset()
    {
        mSegId = INVALID_SEGMENTID;
        mDocId = INVALID_DOCID;
        mHintType = REDO_UNKNOWN;
        mCachedSegments.clear();
        mCachedDocRange.clear();
        mSubCachedDocRange.clear();
    }
    void SetSegmentId(segmentid_t segId)
    {
        mSegId = segId;
        mHintType = REDO_USE_HINT_DOC;
    }
    segmentid_t GetSegmentId() const { return mSegId; }
    void SetLocalDocId(docid_t localDocId)
    {
        mDocId = localDocId;
        mHintType = REDO_USE_HINT_DOC;
    }
    docid_t GetLocalDocId() const { return mDocId; }

    void SetHintType(HintType hintType) { mHintType = hintType; }

    HintType GetHintType() const { return mHintType; }

    void AddCachedSegment(segmentid_t segId) { mCachedSegments.push_back(segId); }
    const std::vector<segmentid_t>& GetCachedSegmentIds() const { return mCachedSegments; }
    void SetCachedDocRanges(std::vector<std::pair<docid_t, docid_t>> docRanges,
                            std::vector<std::pair<docid_t, docid_t>> subDocRanges)
    {
        mCachedDocRange = docRanges;
        mSubCachedDocRange = subDocRanges;
    }
    std::pair<std::vector<std::pair<docid_t, docid_t>>, std::vector<std::pair<docid_t, docid_t>>>
    GetCachedDocRange() const
    {
        return std::make_pair(mCachedDocRange, mSubCachedDocRange);
    }

private:
    segmentid_t mSegId;
    docid_t mDocId;
    HintType mHintType;
    std::vector<segmentid_t> mCachedSegments;
    std::vector<std::pair<docid_t, docid_t>> mCachedDocRange;
    std::vector<std::pair<docid_t, docid_t>> mSubCachedDocRange;
};

DEFINE_SHARED_PTR(OperationRedoHint);
}} // namespace indexlib::partition

#endif //__INDEXLIB_OPERATION_REDO_HINT_H
