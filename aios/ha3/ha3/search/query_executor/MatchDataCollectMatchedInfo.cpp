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
#include "ha3/search/MatchDataCollectMatchedInfo.h"

#include <cstddef>

#include "alog/Logger.h"
#include "autil/MultiValueCreator.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/search/ExecutorMatched.h"
#include "ha3/search/QueryExecutor.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"

namespace matchdoc {
class MatchDoc;
} // namespace matchdoc

using namespace std;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, MatchDataCollectMatchedInfo);

MatchDataCollectMatchedInfo::MatchDataCollectMatchedInfo(
    const std::string name, std::vector<std::vector<size_t>> *rowIdInfo)
    : _colName(name)
    , _rowIdInfo(rowIdInfo) {}

bool MatchDataCollectMatchedInfo::init() {
    _ref = _matchDocAllocator->declare<autil::MultiUInt64>(_colName, SL_ATTRIBUTE);
    return (_ref != nullptr);
}

void MatchDataCollectMatchedInfo::collect(QueryExecutor *executor,
                                          const matchdoc::MatchDoc &matchDoc) {
    vector<size_t> rowID;
    _executorQueue.clear();
    search::ExecutorMatched executorMatched;
    _executorQueue.push_back(executor);
    while (!_executorQueue.empty()) {
        auto cur = _executorQueue.front();
        _executorQueue.pop_front();
        executorMatched.reset();
        cur->accept(&executorMatched);
        const auto &result = executorMatched.getMatchedExecutor();
        if (result.isTermExecutor) {
            size_t idx = result.leafId;
            if (idx < _rowIdInfo->size()) {
                auto ids = (*_rowIdInfo)[result.leafId];
                rowID.insert(rowID.end(), ids.begin(), ids.end());
            } else {
                AUTIL_LOG(ERROR, "leafId exceed rowIdInfo's size");
                return;
            }
        } else {
            _executorQueue.insert(_executorQueue.end(),
                                  result.matchedQueryExecutor.begin(),
                                  result.matchedQueryExecutor.end());
        }
    }
    fillMatchData(rowID, matchDoc);
}

void MatchDataCollectMatchedInfo::fillMatchData(const vector<size_t> &rowID,
                                                const matchdoc::MatchDoc &matchDoc) {
    auto &data = _ref->getReference(matchDoc);
    data.init(autil::MultiValueCreator::createMultiValueBuffer(rowID, _pool));
}

} // namespace search
} // namespace isearch
