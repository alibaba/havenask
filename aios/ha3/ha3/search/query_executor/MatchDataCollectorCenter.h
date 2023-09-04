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
#include <vector>

#include "ha3/search/MatchDataCollectorBase.h"

namespace matchdoc {
class MatchDoc;
} // namespace matchdoc

namespace isearch {
namespace search {
class QueryExecutor;

class MatchDataCollectorCenter {
public:
    MatchDataCollectorCenter() = default;
    ~MatchDataCollectorCenter() = default;
    void reset() {
        _matchDataCollectorVec.clear();
    }
    void subscribe(MatchDataCollectorBase *matchDataCollector) {
        _matchDataCollectorVec.push_back(matchDataCollector);
    }
    void collectAll(search::QueryExecutor *queryExecutor, const matchdoc::MatchDoc &matchDoc) {
        for (auto p : _matchDataCollectorVec) {
            p->collect(queryExecutor, matchDoc);
        }
    }
    bool isEmpty() {
        return _matchDataCollectorVec.empty();
    }

private:
    std::vector<MatchDataCollectorBase *> _matchDataCollectorVec;
};

typedef std::shared_ptr<MatchDataCollectorCenter> MatchDataCollectorCenterPtr;

} // namespace search
} // namespace isearch
