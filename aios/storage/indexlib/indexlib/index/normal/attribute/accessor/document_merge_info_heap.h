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
#include <queue>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/document_merge_info.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class DocumentMergeInfoHeap
{
public:
    DocumentMergeInfoHeap();
    ~DocumentMergeInfoHeap();

public:
    void Init(const index_base::SegmentMergeInfos& segMergeInfos, const ReclaimMapPtr& reclaimMap);
    bool GetNext(DocumentMergeInfo& docMergeInfo);
    bool IsEmpty() const { return mCurrentDocInfo.newDocId == INVALID_DOCID; }

    const DocumentMergeInfo& Top() const { return mCurrentDocInfo; }

    const ReclaimMapPtr& GetReclaimMap() const { return mReclaimMap; }

    DocumentMergeInfoHeap* Clone()
    {
        DocumentMergeInfoHeap* heap = new DocumentMergeInfoHeap;
        heap->Init(mSegMergeInfos, mReclaimMap);
        return heap;
    }

private:
    void LoadNextDoc();

private:
    index_base::SegmentMergeInfos mSegMergeInfos;
    ReclaimMapPtr mReclaimMap;
    std::vector<docid_t> mDocIdCursors;

    size_t mSegCursor;
    DocumentMergeInfo mCurrentDocInfo;
    std::vector<docid_t> mNextNewLocalDocIds;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentMergeInfoHeap);
}} // namespace indexlib::index
