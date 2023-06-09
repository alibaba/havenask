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
#include "ha3/monitor/SessionMetricsReporter.h"

#include <assert.h>
#include <iosfwd>


#include "ha3/isearch.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/proto/BasicDefs.pb.h"

using namespace std;
using namespace isearch::monitor;
using namespace isearch::proto;
namespace isearch {
namespace monitor {

SessionMetricsReporter::SessionMetricsReporter() {
    _role = ROLE_UNKNOWN;
    _phaseNumber = SEARCH_PHASE_ONE;
}

SessionMetricsReporter::~SessionMetricsReporter() {
}

bool SessionMetricsReporter::reportMetrics() {
    assert(_phaseNumber == SEARCH_PHASE_ONE
           || _phaseNumber == SEARCH_PHASE_TWO);
    assert(_metricsCollectPtr);
    switch (_role) {
    case ROLE_SEARCHER:
        if (SEARCH_PHASE_ONE == _phaseNumber) {
            reportPhase1SearcherMetrics();
        } else {
            reportPhase2SearcherMetrics();
        }
        break;
    case ROLE_PROXY:
        if (SEARCH_PHASE_ONE == _phaseNumber) {
            reportPhase1ProxyMetrics();
        } else {
            reportPhase2ProxyMetrics();
        }
        break;
    case ROLE_QRS:
        reportQrsMetrics();
        break;
    default:
        return false;
    }
    return true;
}

} // namespace monitor
} // namespace isearch
