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
#include "ha3/sql/ops/scan/Ha3ScanIterator.h"

#include <memory>

#include "ha3/search/FilterWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/SingleLayerSearcher.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/scan/ScanIterator.h"
#include "indexlib/index/common/ErrorCode.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"

using autil::Result;

namespace isearch {
namespace sql {

Ha3ScanIterator::Ha3ScanIterator(const Ha3ScanIteratorParam &param)
    : ScanIterator(param.matchDocAllocator, param.timeoutTerminator)
    , _needSubDoc(param.matchDocAllocator->hasSubDocAllocator())
    , _eof(false)
    , _filterWrapper(param.filterWrapper)
    , _queryExecutor(param.queryExecutors[0])
    , _delMapReader(param.delMapReader)
    , _subDelMapReader(param.subDelMapReader)
    , _matchDataCollectorCenter(nullptr) {
    if (param.layerMetas[0]) {
        _layerMeta.reset(new search::LayerMeta(*param.layerMetas[0]));
    }
    if (param.matchDataManager) {
        _matchDataCollectorCenter = &param.matchDataManager->getMatchDataCollectorCenter();
    }
    _singleLayerSearcher.reset(new search::SingleLayerSearcher(_queryExecutor.get(),
                                                               _layerMeta.get(),
                                                               _filterWrapper.get(),
                                                               _delMapReader.get(),
                                                               _matchDocAllocator.get(),
                                                               param.timeoutTerminator,
                                                               param.mainToSubIt,
                                                               param.subDelMapReader.get(),
                                                               param.matchDataManager.get(),
                                                               param.needAllSubDocFlag));
}

Ha3ScanIterator::~Ha3ScanIterator() {
}

uint32_t Ha3ScanIterator::getTotalScanCount() {
    return _singleLayerSearcher->getSeekTimes();
}

uint32_t Ha3ScanIterator::getTotalSeekDocCount() {
    return _singleLayerSearcher->getSeekDocCount();
}

bool Ha3ScanIterator::useTruncate() {
    return _queryExecutor->getMainChainDF() / (double)_queryExecutor->getCurrentDF() > 1.0;
}

Result<bool> Ha3ScanIterator::batchSeek(size_t batchSize,
                                        std::vector<matchdoc::MatchDoc> &matchDocs) {
    if (_eof) {
        return true;
    }
    if (batchSize == 0) {
        batchSize = DEFAULT_BATCH_COUNT;
    }
    matchdoc::MatchDoc doc;
    indexlib::index::ErrorCode ec;
    for (size_t i = 0; i < batchSize; ++i) {
        ec = _singleLayerSearcher->seek(_needSubDoc, doc);
        if (ec != indexlib::index::ErrorCode::OK) {
            return false;
        }
        if (matchdoc::INVALID_MATCHDOC == doc) {
            _eof = true;
            return true;
        }
        matchDocs.push_back(doc);
    }
    return false;
}

} // namespace sql
} // namespace isearch
