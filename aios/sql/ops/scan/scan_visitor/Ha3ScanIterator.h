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
#include <stddef.h>
#include <stdint.h>
#include <vector>

#include "autil/result/Result.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/MatchDataManager.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/SingleLayerSearcher.h"
#include "indexlib/index/partition_info.h"
#include "matchdoc/MatchDocAllocator.h"
#include "sql/ops/scan/ScanIterator.h"

namespace indexlib::index {
class DeletionMapReaderAdaptor;
}

namespace indexlib {
namespace index {
class JoinDocidAttributeIterator;
} // namespace index
} // namespace indexlib
namespace matchdoc {
class MatchDoc;
} // namespace matchdoc

namespace isearch {
namespace search {
class MatchDataCollectorCenter;
} // namespace search
} // namespace isearch

namespace sql {

struct Ha3ScanIteratorParam {
    Ha3ScanIteratorParam()
        : timeoutTerminator(NULL)
        , needAllSubDocFlag(false) {}
    std::vector<isearch::search::QueryExecutorPtr> queryExecutors;
    isearch::search::FilterWrapperPtr filterWrapper;
    matchdoc::MatchDocAllocatorPtr matchDocAllocator;
    std::shared_ptr<indexlib::index::DeletionMapReaderAdaptor> delMapReader;
    indexlib::index::DeletionMapReaderPtr subDelMapReader;
    std::vector<isearch::search::LayerMetaPtr> layerMetas;
    indexlib::index::JoinDocidAttributeIterator *mainToSubIt;
    isearch::common::TimeoutTerminator *timeoutTerminator;
    isearch::search::MatchDataManagerPtr matchDataManager;
    bool needAllSubDocFlag;
};

class Ha3ScanIterator : public ScanIterator {
public:
    Ha3ScanIterator(const Ha3ScanIteratorParam &param);
    virtual ~Ha3ScanIterator();

public:
    autil::Result<bool> batchSeek(size_t batchSize,
                                  std::vector<matchdoc::MatchDoc> &matchDocs) override;
    bool useTruncate() override;
    uint32_t getTotalScanCount() override;
    uint32_t getTotalSeekDocCount() override;

private:
    bool _needSubDoc;
    bool _eof;
    isearch::search::FilterWrapperPtr _filterWrapper;
    isearch::search::QueryExecutorPtr _queryExecutor;
    std::shared_ptr<indexlib::index::DeletionMapReaderAdaptor> _delMapReader;
    indexlib::index::DeletionMapReaderPtr _subDelMapReader;
    isearch::search::LayerMetaPtr _layerMeta; // hold resource for queryexecutor use raw pointer
    isearch::search::SingleLayerSearcherPtr _singleLayerSearcher;
    isearch::search::MatchDataCollectorCenter *_matchDataCollectorCenter;
};

typedef std::shared_ptr<Ha3ScanIterator> Ha3ScanIteratorPtr;
} // namespace sql
