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
#include "sql/ops/scan/QueryScanIterator.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/PoolVector.h"
#include "autil/result/Errors.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/isearch.h"
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/QueryExecutor.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader_adaptor.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "sql/common/Log.h"
#include "sql/ops/scan/ScanIterator.h"

using namespace std;
using autil::Result;
using autil::result::RuntimeError;

namespace sql {

QueryScanIterator::QueryScanIterator(
    const isearch::search::QueryExecutorPtr &queryExecutor,
    const isearch::search::FilterWrapperPtr &filterWrapper,
    const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
    const shared_ptr<indexlib::index::DeletionMapReaderAdaptor> &delMapReader,
    const isearch::search::LayerMetaPtr &layerMeta,
    isearch::common::TimeoutTerminator *timeoutTerminator)
    : ScanIterator(matchDocAllocator, timeoutTerminator)
    , _queryExecutor(queryExecutor)
    , _filterWrapper(filterWrapper)
    , _deletionMapReader(delMapReader)
    , _layerMeta(layerMeta)
    , _curDocId((*layerMeta)[0].nextBegin)
    , _curBegin((*layerMeta)[0].nextBegin)
    , _curEnd((*layerMeta)[0].end)
    , _rangeCursor(0)
    , _batchFilterTime(0)
    , _scanTime(0) {}

QueryScanIterator::~QueryScanIterator() {}

Result<bool> QueryScanIterator::batchSeek(size_t batchSize,
                                          std::vector<matchdoc::MatchDoc> &matchDocs) {
    if (_curDocId == END_DOCID) {
        return true;
    }
    constexpr size_t DEFAULT_SCAN_BATCH = 1024;
    autil::ScopedTime2 batchSeekTimer;
    if (batchSize == 0) {
        batchSize = DEFAULT_SCAN_BATCH;
    }
    size_t docCount = 0;
    std::vector<int32_t> docIds;
    size_t minBatchSize = std::min(batchSize, (size_t)DEFAULT_SCAN_BATCH);
    docIds.reserve(minBatchSize);
    docid_t docId = _curDocId;
    assert(docId >= _curBegin);
    indexlib::index::ErrorCode ec;
    while (true) {
        if (_timeoutTerminator && _timeoutTerminator->checkTimeout()) {
            SQL_LOG(WARN, "query executor is timeout.");
            _isTimeout = true;
            break;
        }
        ec = _queryExecutor->seekWithoutCheck(docId, docId);
        AR_REQUIRE_TRUE(ec == indexlib::index::ErrorCode::OK,
                        RuntimeError::make("query executor has error."));
        if (unlikely(!tryToMakeItInRange(docId))) {
            if (docId == END_DOCID) {
                break;
            }
            continue;
        }
        _totalScanCount++;
        if (_deletionMapReader && _deletionMapReader->IsDeleted(docId)) {
            ++docId;
            continue;
        }
        docIds.push_back(docId);
        ++docId;
        if (docIds.size() >= minBatchSize) {
            docCount += batchFilter(docIds, matchDocs);
            docIds.clear();
            if (docCount >= batchSize) {
                break;
            }
        }
    }
    if (_isTimeout) {
        return isearch::common::TimeoutError::make("QueryScan");
    }
    if (docIds.size() > 0) {
        docCount += batchFilter(docIds, matchDocs);
    }
    _curDocId = docId;
    _scanTime += batchSeekTimer.done_us();
    return _curDocId == END_DOCID;
}

size_t QueryScanIterator::batchFilter(const std::vector<int32_t> &docIds,
                                      std::vector<matchdoc::MatchDoc> &matchDocs) {
    autil::ScopedTime2 batchFilterTimer;
    std::vector<matchdoc::MatchDoc> allocateDocs = _matchDocAllocator->batchAllocate(docIds);
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
        _matchDocAllocator->deallocate(deallocateDocs.data(), deallocateDocs.size());
    } else {
        count = allocateDocs.size();
        matchDocs.insert(matchDocs.end(), allocateDocs.begin(), allocateDocs.end());
    }
    _batchFilterTime += batchFilterTimer.done_us();
    return count;
}

bool QueryScanIterator::moveToCorrectRange(docid_t &docId) {
    while (++_rangeCursor < _layerMeta->size()) {
        if (docId <= (*_layerMeta)[_rangeCursor].end) {
            _curBegin = (*_layerMeta)[_rangeCursor].begin;
            _curEnd = (*_layerMeta)[_rangeCursor].end;
            if (docId < _curBegin) {
                docId = _curBegin;
                return false;
            } else {
                return true;
            }
        }
    }
    docId = END_DOCID;
    return false;
}

uint32_t QueryScanIterator::getTotalSeekedCount() const {
    uint32_t docCount = 0;
    for (size_t cursor = 0; cursor < _rangeCursor; ++cursor) {
        docCount += (*_layerMeta)[cursor].end - (*_layerMeta)[cursor].begin;
    }
    if (_rangeCursor < _layerMeta->size()) { // accumulate for last cursor
        docCount += _curDocId - _curBegin;
    }
    return docCount;
}

uint32_t QueryScanIterator::getTotalWholeDocCount() const {
    uint32_t docCount = 0;
    for (size_t cursor = 0; cursor < _layerMeta->size(); ++cursor) {
        docCount += (*_layerMeta)[cursor].end - (*_layerMeta)[cursor].begin;
    }
    return docCount;
}

} // namespace sql
