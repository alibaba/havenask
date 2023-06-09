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
#include "indexlib/index/inverted_index/truncate/NoSortTruncateCollector.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(index, NoSortTruncateCollector);

NoSortTruncateCollector::NoSortTruncateCollector(uint64_t minDocCountToReserve, uint64_t maxDocCountToReserve,
                                                 const std::shared_ptr<IDocFilterProcessor>& filterProcessor,
                                                 const std::shared_ptr<DocDistinctor>& docDistinctor,
                                                 const std::shared_ptr<BucketVectorAllocator>& bucketVecAllocator)
    : DocCollector(minDocCountToReserve, maxDocCountToReserve, filterProcessor, docDistinctor, bucketVecAllocator)
{
}

void NoSortTruncateCollector::ReInit() {}

void NoSortTruncateCollector::DoCollect(const std::shared_ptr<PostingIterator>& postingIt)
{
    docid_t docId = 0;
    while ((docId = postingIt->SeekDoc(docId)) != INVALID_DOCID) {
        if (_filterProcessor != nullptr && _filterProcessor->IsFiltered(docId)) {
            continue;
        }
        if (_docDistinctor == nullptr) {
            if (_docIdVec->size() < _minDocCountToReserve) {
                _docIdVec->push_back(docId);
            } else {
                return;
            }
        } else {
            if (_docIdVec->size() >= _maxDocCountToReserve) {
                return;
            } else if (!_docDistinctor->IsFull()) {
                _docDistinctor->Distinct(docId);
                _docIdVec->push_back(docId);
            } else if (_docIdVec->size() < _minDocCountToReserve) {
                _docIdVec->push_back(docId);
            } else {
                return;
            }
        }
    }
}

void NoSortTruncateCollector::Truncate()
{
    if (_docIdVec->size() > 0) {
        _minValueDocId = _docIdVec->back(); // useless, set it for case.
    }
}

} // namespace indexlib::index
