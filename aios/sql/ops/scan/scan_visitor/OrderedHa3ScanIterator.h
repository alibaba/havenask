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
#include <queue>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/result/Result.h"
#include "ha3/rank/Comparator.h"
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/SingleLayerSearcher.h"
#include "indexlib/index/partition_info.h"
#include "matchdoc/MatchDoc.h"
#include "sql/ops/scan/ScanIterator.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace indexlib {
namespace index {
class DeletionMapReaderAdaptor;
} // namespace index
} // namespace indexlib

namespace suez {
namespace turing {
class AttributeExpression;
class AttributeExpressionCreator;
} // namespace turing
} // namespace suez

namespace isearch {
namespace search {
class MatchDataCollectorCenter;
} // namespace search
} // namespace isearch

namespace sql {

class SortInitParam;
struct Ha3ScanIteratorParam;

class OrderedHa3ScanIterator : public ScanIterator {
public:
    OrderedHa3ScanIterator(const Ha3ScanIteratorParam &param,
                           autil::mem_pool::Pool *pool,
                           const SortInitParam &sortDesc,
                           suez::turing::AttributeExpressionCreator *attributeExpressionCreator);
    virtual ~OrderedHa3ScanIterator();

public:
    bool init();
    autil::Result<bool> batchSeek(size_t batchSize,
                                  std::vector<matchdoc::MatchDoc> &matchDocs) override;
    uint32_t getTotalScanCount() override;

    class RangeComp {
    public:
        RangeComp(const isearch::rank::Comparator *comp)
            : _comp(comp) {}

    public:
        bool operator()(const std::pair<matchdoc::MatchDoc, size_t> &lft,
                        const std::pair<matchdoc::MatchDoc, size_t> &rht) const {
            return _comp->compare(lft.first, rht.first);
        }

    private:
        const isearch::rank::Comparator *_comp;
    };
    typedef std::priority_queue<std::pair<matchdoc::MatchDoc, size_t>,
                                std::vector<std::pair<matchdoc::MatchDoc, size_t>>,
                                RangeComp>
        RangePriorityQueueType;

private:
    bool seekAndPushQueue(size_t i, RangePriorityQueueType &rangePriorityQueue);

private:
    autil::mem_pool::Pool *_pool;
    size_t _sortLimit;
    std::vector<std::string> _refNames;
    std::vector<bool> _orders;
    std::vector<suez::turing::AttributeExpression *> _sortDescExpr;
    bool _needSubDoc;
    std::vector<matchdoc::MatchDoc> _matchDocs;
    isearch::search::FilterWrapperPtr _filterWrapper;
    std::vector<isearch::search::QueryExecutorPtr> _queryExecutors;
    std::shared_ptr<indexlib::index::DeletionMapReaderAdaptor> _delMapReader;
    indexlib::index::DeletionMapReaderPtr _subDelMapReader;
    std::vector<isearch::search::LayerMetaPtr>
        _layerMetas; // hold resource for queryexecutor use raw pointer
    isearch::search::MatchDataCollectorCenter *_matchDataCollectorCenter;
    std::vector<isearch::search::SingleLayerSearcherPtr> _orderedSingleLayerSearcher;
    std::vector<isearch::search::SingleLayerSearcherPtr> _unorderedSingleLayerSearcher;
    suez::turing::AttributeExpressionCreator *_attributeExpressionCreator;
    isearch::rank::Comparator *_comp;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<OrderedHa3ScanIterator> OrderedHa3ScanIteratorPtr;
} // namespace sql
