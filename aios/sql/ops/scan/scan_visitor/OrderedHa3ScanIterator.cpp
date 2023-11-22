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
#include "sql/ops/scan/OrderedHa3ScanIterator.h"

#include <assert.h>
#include <ext/alloc_traits.h>
#include <iosfwd>
#include <memory>

#include "autil/mem_pool/PoolBase.h"
#include "autil/mem_pool/PoolVector.h"
#include "autil/result/Errors.h"
#include "ha3/rank/ComboComparator.h"
#include "ha3/rank/MatchDocPriorityQueue.h"
#include "ha3/search/Filter.h"
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/MatchDataManager.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/SingleLayerSearcher.h"
#include "indexlib/index/common/ErrorCode.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/Trait.h"
#include "sql/common/Log.h"
#include "sql/ops/scan/Ha3ScanIterator.h"
#include "sql/ops/scan/MatchDocComparatorCreator.h"
#include "sql/ops/scan/ScanIterator.h"
#include "sql/ops/sort/SortInitParam.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace isearch::search;
using namespace isearch::rank;

using autil::Result;
using autil::result::RuntimeError;

namespace sql {
AUTIL_LOG_SETUP(sql, OrderedHa3ScanIterator);

OrderedHa3ScanIterator::OrderedHa3ScanIterator(
    const Ha3ScanIteratorParam &param,
    autil::mem_pool::Pool *pool,
    const SortInitParam &sortDesc,
    AttributeExpressionCreator *attributeExpressionCreator)
    : ScanIterator(param.matchDocAllocator, param.timeoutTerminator)
    , _pool(pool)
    , _sortLimit(sortDesc.topk)
    , _refNames(sortDesc.keys)
    , _orders(sortDesc.orders)
    , _needSubDoc(param.matchDocAllocator->hasSubDocAllocator())
    , _filterWrapper(param.filterWrapper)
    , _queryExecutors(param.queryExecutors)
    , _delMapReader(param.delMapReader)
    , _subDelMapReader(param.subDelMapReader)
    , _layerMetas(param.layerMetas)
    , _matchDataCollectorCenter(nullptr)
    , _attributeExpressionCreator(attributeExpressionCreator)
    , _comp(nullptr) {
    if (param.matchDataManager) {
        _matchDataCollectorCenter = &param.matchDataManager->getMatchDataCollectorCenter();
    }
    for (size_t i = 0; i < _layerMetas.size(); ++i) {
        SingleLayerSearcherPtr singleLayerSearcher(
            new SingleLayerSearcher(_queryExecutors[i].get(),
                                    _layerMetas[i].get(),
                                    _filterWrapper.get(),
                                    _delMapReader.get(),
                                    _matchDocAllocator.get(),
                                    _timeoutTerminator,
                                    param.mainToSubIt,
                                    param.subDelMapReader.get(),
                                    param.matchDataManager.get(),
                                    param.needAllSubDocFlag));
        if ((*_layerMetas[i])[0].ordered == DocIdRangeMeta::OT_ORDERED) {
            _orderedSingleLayerSearcher.emplace_back(singleLayerSearcher);
        } else if ((*_layerMetas[i])[0].ordered == DocIdRangeMeta::OT_UNORDERED) {
            _unorderedSingleLayerSearcher.emplace_back(singleLayerSearcher);
        } else {
            assert(false);
        }
    }
    SQL_LOG(TRACE2,
            "order single layer searcher [%lu], unordered single layer searcher [%lu]",
            _orderedSingleLayerSearcher.size(),
            _unorderedSingleLayerSearcher.size());
}

OrderedHa3ScanIterator::~OrderedHa3ScanIterator() {
    if (_comp != nullptr) {
        POOL_DELETE_CLASS(_comp);
    }
}

bool OrderedHa3ScanIterator::init() {
    for (const auto &refName : _refNames) {
        auto attrExpr = _attributeExpressionCreator->createAttributeExpression(refName);
        SQL_LOG(DEBUG, "create sort attr expr [%s]", refName.c_str());
        if (attrExpr == nullptr) {
            SQL_LOG(ERROR, "create attr expr [%s] failed", refName.c_str());
            return false;
        }
        if (!attrExpr->allocate(_matchDocAllocator.get())) {
            SQL_LOG(ERROR, "allocate attr expr [%s] failed", refName.c_str());
            return false;
        }
        auto ref = attrExpr->getReferenceBase();
        ref->setSerializeLevel(SL_ATTRIBUTE);
        _sortDescExpr.emplace_back(attrExpr);
    }
    _matchDocAllocator->extend();
    return true;
}

uint32_t OrderedHa3ScanIterator::getTotalScanCount() const {
    uint32_t totalScanCount = 0;
    for (const auto &singleLayerSearcher : _orderedSingleLayerSearcher) {
        totalScanCount += singleLayerSearcher->getSeekTimes();
    }
    for (const auto &singleLayerSearcher : _unorderedSingleLayerSearcher) {
        totalScanCount += singleLayerSearcher->getSeekTimes();
    }
    return totalScanCount;
}

uint32_t OrderedHa3ScanIterator::getTotalSeekedCount() const {
    uint32_t docCount = 0;
    for (const auto &singleLayerSearcher : _orderedSingleLayerSearcher) {
        docCount += singleLayerSearcher->getSeekedCount();
    }
    for (const auto &singleLayerSearcher : _unorderedSingleLayerSearcher) {
        docCount += singleLayerSearcher->getSeekedCount();
    }
    return docCount;
}

uint32_t OrderedHa3ScanIterator::getTotalWholeDocCount() const {
    uint32_t docCount = 0;
    for (const auto &singleLayerSearcher : _orderedSingleLayerSearcher) {
        docCount += singleLayerSearcher->getWholeDocCount();
    }
    for (const auto &singleLayerSearcher : _unorderedSingleLayerSearcher) {
        docCount += singleLayerSearcher->getWholeDocCount();
    }
    return docCount;
}

Result<bool> OrderedHa3ScanIterator::batchSeek(size_t batchSize, vector<MatchDoc> &matchDocs) {
    // build sort desc
    // unordered seek to build docs priority queue with sort desc
    MatchDoc doc;
    indexlib::index::ErrorCode ec;
    vector<MatchDoc> tmpMatchDocs;
    for (auto singleLayerSearcher : _unorderedSingleLayerSearcher) {
        while (true) {
            ec = singleLayerSearcher->seek(_needSubDoc, doc);
            if (ec != indexlib::index::ErrorCode::OK) {
                return false;
            }
            if (matchdoc::INVALID_MATCHDOC == doc) {
                break;
            }
            tmpMatchDocs.emplace_back(doc);
        }
    }
    for (auto attr : _sortDescExpr) {
        attr->batchEvaluate(tmpMatchDocs.data(), tmpMatchDocs.size());
    }
    MatchDocComparatorCreator matchDocComparatorCreator(_pool, _matchDocAllocator.get());
    _comp = matchDocComparatorCreator.createComparator(_refNames, _orders);
    AR_REQUIRE_TRUE(_comp, RuntimeError::make("create sort comparator failed"));
    MatchDocPriorityQueue docPriorityQueue(_sortLimit, _pool, _comp);
    MatchDoc tmpDoc = matchdoc::INVALID_MATCHDOC;
    ;
    for (auto doc : tmpMatchDocs) {
        docPriorityQueue.push(doc, &tmpDoc);
    }

    // build ordered singleLayerSearcher priority queue
    RangePriorityQueueType rangePriorityQueue((RangeComp(_comp)));
    for (size_t i = 0; i < _orderedSingleLayerSearcher.size(); ++i) {
        if (!seekAndPushQueue(i, rangePriorityQueue)) {
            return false;
        }
    }
    // update docs queue
    while (!rangePriorityQueue.empty()) {
        auto pair = rangePriorityQueue.top();
        if (MatchDocPriorityQueue::ITEM_DENIED == docPriorityQueue.push(pair.first, &tmpDoc)) {
            break;
        }
        rangePriorityQueue.pop();
        if (!seekAndPushQueue(pair.second, rangePriorityQueue)) {
            return false;
        }
    }
    // output sorted docPriorityQueue
    int64_t docSize = docPriorityQueue.count();
    _matchDocs.resize(docSize);
    for (int64_t i = docSize - 1; i >= 0; --i) {
        _matchDocs[i] = docPriorityQueue.top();
        docPriorityQueue.pop();
    }
    // TODO delete tmp doc
    matchDocs.insert(matchDocs.begin(), _matchDocs.begin(), _matchDocs.end());
    return true;
}

bool OrderedHa3ScanIterator::seekAndPushQueue(size_t i,
                                              RangePriorityQueueType &rangePriorityQueue) {
    MatchDoc doc;
    indexlib::index::ErrorCode ec = _orderedSingleLayerSearcher[i]->seek(_needSubDoc, doc);
    if (ec != indexlib::index::ErrorCode::OK) {
        return false;
    }
    if (matchdoc::INVALID_MATCHDOC != doc) {
        for (auto attr : _sortDescExpr) {
            attr->evaluate(doc);
        }
        rangePriorityQueue.push(make_pair(doc, i));
    }
    return true;
}

} // namespace sql
