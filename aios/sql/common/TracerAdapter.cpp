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
#include "sql/common/TracerAdapter.h"

#include <algorithm>
#include <cstdint>
#include <iosfwd>
#include <unordered_map>
#include <utility>

#include "navi/common.h"

using namespace std;
using namespace navi;

namespace sql {
AUTIL_LOG_SETUP(sql, TracerAdapter);

TracerAdapter::TracerAdapter() {}

TracerAdapter::~TracerAdapter() {}

void TracerAdapter::trace(const char *traceInfo) {
    SQL_LOG(DEBUG, "%s", traceInfo);
}

void TracerAdapter::setNaviLevel(LogLevel level) {
    static const std::unordered_map<LogLevel, int32_t> logLevelMap = {
        {LOG_LEVEL_TRACE3, ISEARCH_TRACE_TRACE3},
        {LOG_LEVEL_TRACE2, ISEARCH_TRACE_TRACE2},
        {LOG_LEVEL_TRACE1, ISEARCH_TRACE_TRACE1},
        {LOG_LEVEL_DEBUG, ISEARCH_TRACE_DEBUG},
        {LOG_LEVEL_INFO, ISEARCH_TRACE_INFO},
        {LOG_LEVEL_WARN, ISEARCH_TRACE_WARN},
        {LOG_LEVEL_ERROR, ISEARCH_TRACE_ERROR},
        {LOG_LEVEL_FATAL, ISEARCH_TRACE_FATAL},
    };
    if (level > LOG_LEVEL_TRACE3) {
        setLevel(ISEARCH_TRACE_TRACE3);
    } else {
        auto it = logLevelMap.find(level);
        if (it != logLevelMap.end()) {
            setLevel(it->second);
        } else {
            setLevel(ISEARCH_TRACE_FATAL);
        }
    }
}

} // namespace sql
