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
#include "indexlib/index/merger_util/truncate/no_sort_truncate_collector.h"

using namespace std;

namespace indexlib::index::legacy {
IE_LOG_SETUP(index, NoSortTruncateCollector);

NoSortTruncateCollector::NoSortTruncateCollector(uint64_t minDocCountToReserve, uint64_t maxDocCountToReserve,
                                                 const DocFilterProcessorPtr& filterProcessor,
                                                 const DocDistinctorPtr& docDistinctor,
                                                 const BucketVectorAllocatorPtr& bucketVecAllocator)
    : DocCollector(minDocCountToReserve, maxDocCountToReserve, filterProcessor, docDistinctor, bucketVecAllocator)
{
}

NoSortTruncateCollector::~NoSortTruncateCollector() {}

void NoSortTruncateCollector::ReInit() {}

void NoSortTruncateCollector::DoCollect(const index::DictKeyInfo& key, const PostingIteratorPtr& postingIt)
{
    DocFilterProcessor* filter = mFilterProcessor.get();
    DocDistinctor* docDistinctor = mDocDistinctor.get();
    DocIdVector& docIdVec = *mDocIdVec;
    uint32_t maxDocCountToReserve = mMaxDocCountToReserve;
    uint32_t minDocCountToReserve = mMinDocCountToReserve;
    docid_t docId = 0;
    while ((docId = postingIt->SeekDoc(docId)) != INVALID_DOCID) {
        if (filter && filter->IsFiltered(docId)) {
            continue;
        }
        if (!docDistinctor) {
            if (docIdVec.size() < minDocCountToReserve) {
                docIdVec.push_back(docId);
            } else {
                return;
            }
        } else {
            if (docIdVec.size() >= maxDocCountToReserve) {
                return;
            } else if (!docDistinctor->IsFull()) {
                docDistinctor->Distinct(docId);
                docIdVec.push_back(docId);
            } else if (docIdVec.size() < minDocCountToReserve) {
                docIdVec.push_back(docId);
            } else {
                return;
            }
        }
    }
}

void NoSortTruncateCollector::Truncate()
{
    if (mDocIdVec->size() > 0) {
        mMinValueDocId = mDocIdVec->back(); // useless, set it for case.
    }
}
} // namespace indexlib::index::legacy
