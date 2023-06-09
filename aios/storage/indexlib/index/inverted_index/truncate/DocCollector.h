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

#include "indexlib/index/inverted_index/truncate/BucketVectorAllocator.h"
#include "indexlib/index/inverted_index/truncate/DocDistinctor.h"
#include "indexlib/index/inverted_index/truncate/IDocFilterProcessor.h"

namespace indexlib::index {

class DocCollector
{
public:
    using DocIdVector = BucketVectorAllocator::DocIdVector;
    using BucketVector = BucketVectorAllocator::BucketVector;

public:
    DocCollector(uint64_t minDocCountToReserve, uint64_t maxDocCountToReserve,
                 const std::shared_ptr<IDocFilterProcessor>& filterProcessor,
                 const std::shared_ptr<DocDistinctor>& docDistinctor,
                 const std::shared_ptr<BucketVectorAllocator>& bucketVecAllocator)
        : _minDocCountToReserve(minDocCountToReserve)
        , _maxDocCountToReserve(maxDocCountToReserve)
        , _filterProcessor(filterProcessor)
        , _docDistinctor(docDistinctor)
        , _bucketVecAllocator(bucketVecAllocator)
    {
    }
    virtual ~DocCollector() = default;

public:
    void CollectDocIds(const DictKeyInfo& key, const std::shared_ptr<PostingIterator>& postingIt, df_t docFreq)
    {
        assert(postingIt);
        int64_t docIdVecReserveSize = (int64_t)_minDocCountToReserve;

        if (docFreq >= 0 && docFreq < docIdVecReserveSize) {
            docIdVecReserveSize = docFreq;
        }
        _docIdVec = _bucketVecAllocator->AllocateDocVector();
        _docIdVec->clear();
        _docIdVec->reserve(docIdVecReserveSize);
        _minValueDocId = INVALID_DOCID;
        if (_filterProcessor) {
            _filterProcessor->BeginFilter(key, postingIt);
        }
        ReInit();
        DoCollect(postingIt);
        Truncate();
        if (_docDistinctor) {
            _docDistinctor->Reset();
        }
    }

    virtual int64_t EstimateMemoryUse(uint32_t docCount) const
    {
        int64_t size = sizeof(docid_t) * _maxDocCountToReserve * 2; // _docIdVec
        if (_docDistinctor) {
            size += _docDistinctor->EstimateMemoryUse(_maxDocCountToReserve, docCount);
        }
        return size;
    }

private:
    virtual void ReInit() = 0;
    virtual void DoCollect(const std::shared_ptr<PostingIterator>& postingIt) = 0;
    virtual void Truncate() = 0;

public:
    virtual void Reset()
    {
        if (_docIdVec) {
            _bucketVecAllocator->DeallocateDocIdVec(_docIdVec);
            _docIdVec.reset();
        }
    }
    bool Empty() const { return _docIdVec->size() == 0; }
    const std::shared_ptr<DocIdVector>& GetTruncateDocIds() const { return _docIdVec; }
    docid_t GetMinValueDocId() const { return _minValueDocId; }

protected:
    uint64_t _minDocCountToReserve;
    uint64_t _maxDocCountToReserve;
    docid_t _minValueDocId;
    std::shared_ptr<IDocFilterProcessor> _filterProcessor;
    std::shared_ptr<DocDistinctor> _docDistinctor;
    std::shared_ptr<DocIdVector> _docIdVec;
    std::shared_ptr<BucketVectorAllocator> _bucketVecAllocator;
};

} // namespace indexlib::index
