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
#include "ha3/sql/ops/scan/RangeScanIteratorWithoutFilter.h"

#include <algorithm>
#include <cstdint>
#include <memory>

#include "autil/mem_pool/PoolVector.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/scan/ScanIterator.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader_adaptor.h"
#include "matchdoc/MatchDocAllocator.h"

using autil::Result;

namespace matchdoc {
class MatchDoc;
} // namespace matchdoc

namespace isearch {
namespace sql {

RangeScanIteratorWithoutFilter::RangeScanIteratorWithoutFilter(
    const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
    const std::shared_ptr<indexlib::index::DeletionMapReaderAdaptor> &delMapReader,
    const search::LayerMetaPtr &layerMeta,
    common::TimeoutTerminator *timeoutTerminator)
    : ScanIterator(matchDocAllocator, timeoutTerminator)
    , _delMapReader(delMapReader)
    , _layerMeta(layerMeta)
    , _rangeIdx(0)
    , _curId(-1) {}

Result<bool> RangeScanIteratorWithoutFilter::batchSeek(size_t batchSize,
                                                       std::vector<matchdoc::MatchDoc> &matchDocs) {
    if (batchSize == 0) {
        batchSize = DEFAULT_BATCH_COUNT;
    }
    size_t docCount = 0;
    std::vector<int32_t> docIds;
    docIds.reserve(batchSize);
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
            ++docCount;
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
        return common::TimeoutError::make("RangeScanWithoutFilter");
    }
    if (docIds.size() > 0) {
        matchDocs = _matchDocAllocator->batchAllocate(docIds);
    }
    return _rangeIdx >= _layerMeta->size();
}

} // namespace sql
} // namespace isearch
