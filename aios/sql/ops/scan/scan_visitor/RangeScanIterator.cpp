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
#include "sql/ops/scan/RangeScanIterator.h"

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <memory>
#include <vector>

#include "autil/mem_pool/PoolVector.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader_adaptor.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "sql/common/common.h"
#include "sql/ops/scan/ScanIterator.h"

using autil::Result;

namespace sql {
RangeScanIterator::RangeScanIterator(
    const isearch::search::FilterWrapperPtr &filterWrapper,
    const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
    const std::shared_ptr<indexlib::index::DeletionMapReaderAdaptor> &delMapReader,
    const isearch::search::LayerMetaPtr &layerMeta,
    isearch::common::TimeoutTerminator *timeoutTerminator)
    : ScanIterator(matchDocAllocator, timeoutTerminator)
    , _filterWrapper(filterWrapper)
    , _delMapReader(delMapReader)
    , _layerMeta(layerMeta)
    , _rangeIdx(0)
    , _curId(-1) {}

Result<bool> RangeScanIterator::batchSeek(size_t batchSize,
                                          std::vector<matchdoc::MatchDoc> &matchDocs) {
    if (batchSize == 0) {
        batchSize = DEFAULT_BATCH_COUNT;
    }
    size_t docCount = 0;
    std::vector<int32_t> docIds;
    size_t minBatchSize = std::min(batchSize, (size_t)DEFAULT_BATCH_COUNT);
    docIds.reserve(minBatchSize);
    for (; _rangeIdx < _layerMeta->size(); ++_rangeIdx) {
        auto &range = (*_layerMeta)[_rangeIdx];
        _curId = _curId >= (range.begin - 1) ? _curId : (range.begin - 1);
        while (docCount < batchSize && ++_curId <= range.end) {
            if (_timeoutTerminator && _timeoutTerminator->checkTimeout()) {
                _isTimeout = true;
                break;
            }
            ++_totalScanCount;
            if (_delMapReader && _delMapReader->IsDeleted(_curId)) {
                continue;
            }
            docIds.push_back(_curId);
            if (docIds.size() >= minBatchSize) {
                docCount += batchFilter(docIds, matchDocs);
                docIds.clear();
            }
        }
        if (docCount >= batchSize) {
            break;
        }
        --_curId;
        if (_isTimeout) {
            break;
        }
    }
    if (_isTimeout) {
        return isearch::common::TimeoutError::make("RangeScan");
    }
    if (docIds.size() > 0) {
        docCount += batchFilter(docIds, matchDocs);
    }
    return _rangeIdx >= _layerMeta->size();
}

size_t RangeScanIterator::batchFilter(const std::vector<int32_t> &docIds,
                                      std::vector<matchdoc::MatchDoc> &matchDocs) {
    const std::vector<matchdoc::MatchDoc> &allocateDocs = _matchDocAllocator->batchAllocate(docIds);
    assert(docIds.size() == allocateDocs.size());
    size_t count = 0;
    if (_filterWrapper) {
        std::vector<matchdoc::MatchDoc> deallocateDocs;
        for (auto matchDoc : allocateDocs) {
            if (_filterWrapper->pass(matchDoc)) {
                ++count;
                matchDocs.push_back(matchDoc);
            } else {
                deallocateDocs.push_back(matchDoc);
            }
        }
        for (auto matchDoc : deallocateDocs) {
            _matchDocAllocator->deallocate(matchDoc);
        }
    } else {
        count = allocateDocs.size();
        matchDocs.insert(matchDocs.end(), allocateDocs.begin(), allocateDocs.end());
    }
    return count;
}

} // namespace sql
