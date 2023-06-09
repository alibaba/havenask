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

#include "indexlib/index/inverted_index/truncate/AttributeEvaluator.h"
#include "indexlib/index/inverted_index/truncate/Comparator.h"
#include "indexlib/index/inverted_index/truncate/DocInfoAllocator.h"
#include "indexlib/table/normal_table/index_task/SegmentMergeInfo.h"

namespace indexlib::index {

class WeightedDocIterator
{
public:
    WeightedDocIterator(const indexlibv2::table::SegmentMergeInfo& segMergeInfo,
                        const std::shared_ptr<IEvaluator>& evaluator, const std::shared_ptr<Comparator>& comparator,
                        const std::shared_ptr<DocInfoAllocator>& docInfoAllocator)
        : _segMergeInfo(segMergeInfo)
        , _evaluator(evaluator)
        , _comparator(comparator)
        , _docInfoAllocator(docInfoAllocator)
    {
        _docInfo = _docInfoAllocator->Allocate();
        _docInfo->SetDocId(-1);
        _maxDocId = (docid_t)(_segMergeInfo.segmentInfo.docCount - 1);
    }

    ~WeightedDocIterator() { _docInfoAllocator->Deallocate(_docInfo); }

    bool HasNext() const { return _docInfo->GetDocId() < _maxDocId; }

    docid_t Next()
    {
        if (_docInfo->GetDocId() < _maxDocId) {
            docid_t docId = _docInfo->IncDocId();
            _evaluator->Evaluate(docId, std::shared_ptr<PostingIterator>(), _docInfo);
            return docId;
        }
        return INVALID_DOCID;
    }

    docid_t GetLocalDocId() const { return _docInfo->GetDocId(); }

    docid_t GetBasedocId() const { return _segMergeInfo.baseDocId; }

    const std::shared_ptr<Comparator>& GetComparator() const { return _comparator; }

    segmentid_t GetSegmentId() const { return _segMergeInfo.segmentId; }

    DocInfo* GetDocInfo() const { return _docInfo; }

private:
    DocInfo* _docInfo;
    docid_t _maxDocId;
    indexlibv2::table::SegmentMergeInfo _segMergeInfo;
    std::shared_ptr<IEvaluator> _evaluator;
    std::shared_ptr<Comparator> _comparator;
    std::shared_ptr<DocInfoAllocator> _docInfoAllocator;
};

class WeightedDocIteratorComp
{
public:
    bool operator()(WeightedDocIterator* left, WeightedDocIterator* right) const
    {
        std::shared_ptr<Comparator> cmp = left->GetComparator();
        if (cmp->LessThan(right->GetDocInfo(), left->GetDocInfo())) {
            return true;
        } else if (cmp->LessThan(left->GetDocInfo(), right->GetDocInfo())) {
            return false;
        }
        return left->GetSegmentId() > right->GetSegmentId();
    }
};

} // namespace indexlib::index
