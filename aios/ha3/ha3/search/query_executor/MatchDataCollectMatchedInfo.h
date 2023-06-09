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

#include <stddef.h>
#include <deque>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/MultiValueType.h"
#include "ha3/search/MatchDataCollectorBase.h"

namespace matchdoc {
class MatchDoc;
template <typename T> class Reference;
}  // namespace matchdoc

namespace isearch {
namespace search {
class QueryExecutor;

class MatchDataCollectMatchedInfo: public MatchDataCollectorBase {
public:
    MatchDataCollectMatchedInfo(const std::string name, std::vector<std::vector<size_t>>* rowIdInfo);
    ~MatchDataCollectMatchedInfo() = default;
    bool init();
public:
    void collect(QueryExecutor* executor, const matchdoc::MatchDoc &matchDoc) override;
private:
    void fillMatchData(const std::vector<size_t> &rowID, const matchdoc::MatchDoc &matchDoc);
private:
    std::string _colName;
    std::vector<std::vector<size_t>>* _rowIdInfo;
    std::deque<const QueryExecutor*> _executorQueue;
    matchdoc::Reference<autil::MultiUInt64> *_ref;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace search
} // namespace isearch
