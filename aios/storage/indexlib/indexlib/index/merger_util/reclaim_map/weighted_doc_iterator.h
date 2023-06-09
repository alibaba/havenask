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
#ifndef __INDEXLIB_WEIGHTED_DOC_ITERATOR_H
#define __INDEXLIB_WEIGHTED_DOC_ITERATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/attribute_evaluator.h"
#include "indexlib/index/merger_util/truncate/comparator.h"
#include "indexlib/index/merger_util/truncate/doc_info_allocator.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

class WeightedDocIterator
{
public:
    WeightedDocIterator(const index_base::SegmentMergeInfo& segMergeInfo, legacy::EvaluatorPtr evaluatorPtr,
                        legacy::ComparatorPtr comparatorPtr, legacy::DocInfoAllocatorPtr docInfoAllocator)
        : mSegMergeInfo(segMergeInfo)
        , mEvaluatorPtr(evaluatorPtr)
        , mComparatorPtr(comparatorPtr)
        , mDocInfoAllocator(docInfoAllocator)
    {
        mDocInfo = mDocInfoAllocator->Allocate();
        mDocInfo->SetDocId(-1);
        mMaxDocId = (docid_t)(mSegMergeInfo.segmentInfo.docCount - 1);
    }

    ~WeightedDocIterator() { mDocInfoAllocator->Deallocate(mDocInfo); }

    bool HasNext() const { return mDocInfo->GetDocId() < mMaxDocId; }

    docid_t Next()
    {
        if (mDocInfo->GetDocId() < mMaxDocId) {
            docid_t docId = mDocInfo->IncDocId();
            mEvaluatorPtr->Evaluate(docId, std::shared_ptr<PostingIterator>(), mDocInfo);
            return docId;
        }
        return INVALID_DOCID;
    }

    docid_t GetLocalDocId() const { return mDocInfo->GetDocId(); }

    docid_t GetBasedocId() const { return mSegMergeInfo.baseDocId; }

    const legacy::ComparatorPtr& GetComparator() const { return mComparatorPtr; }

    segmentid_t GetSegmentId() const { return mSegMergeInfo.segmentId; }

    legacy::DocInfo* GetDocInfo() const { return mDocInfo; }

private:
    legacy::DocInfo* mDocInfo;
    docid_t mMaxDocId;
    index_base::SegmentMergeInfo mSegMergeInfo;
    legacy::EvaluatorPtr mEvaluatorPtr;
    legacy::ComparatorPtr mComparatorPtr;
    legacy::DocInfoAllocatorPtr mDocInfoAllocator;

private:
    IE_LOG_DECLARE();
};

class WeightedDocIteratorComp
{
public:
    bool operator()(WeightedDocIterator* left, WeightedDocIterator* right) const
    {
        legacy::ComparatorPtr cmp = left->GetComparator();
        if (cmp->LessThan(right->GetDocInfo(), left->GetDocInfo())) {
            return true;
        } else if (cmp->LessThan(left->GetDocInfo(), right->GetDocInfo())) {
            return false;
        }
        return left->GetSegmentId() > right->GetSegmentId();
    }
};

DEFINE_SHARED_PTR(WeightedDocIterator);
} // namespace indexlib::index::legacy

#endif //__INDEXLIB_WEIGHTED_DOC_ITERATOR_H
