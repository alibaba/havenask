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
#include "ha3/qrs/RequestParserProcessor.h"

#include <assert.h>
#include <stddef.h>
#include <memory>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "ha3/common/ClauseBase.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/MultiErrorResult.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/qrs/QrsProcessor.h"
#include "ha3/queryparser/RequestParser.h"

namespace isearch {
namespace common {
class FetchSummaryClause;
}  // namespace common
}  // namespace isearch

using namespace isearch::config;
using namespace isearch::queryparser;
using namespace isearch::common;

namespace isearch {
namespace qrs {
AUTIL_LOG_SETUP(ha3, RequestParserProcessor);

RequestParserProcessor::RequestParserProcessor(const config::ClusterConfigMapPtr &clusterConfigMapPtr) 
    : _clusterConfigMapPtr(clusterConfigMapPtr)
{ 
}

RequestParserProcessor::~RequestParserProcessor() { 
    
}

void RequestParserProcessor::process(RequestPtr &requestPtr, 
                                     ResultPtr &resultPtr) 
{
    assert(resultPtr != NULL);

    _metricsCollectorPtr->requestParserStartTrigger();
    RequestParser requestParser;
    bool succ = requestParser.parseRequest(requestPtr, _clusterConfigMapPtr);
    if (!succ) {
        AUTIL_LOG(WARN, "ParseRequset Failed: request[%s]",
                requestPtr->getOriginalString().c_str());
        ErrorResult errorResult = requestParser.getErrorResult();
        resultPtr->getMultiErrorResult()->addError(errorResult.getErrorCode(), 
                errorResult.getErrorMsg());
        _metricsCollectorPtr->requestParserEndTrigger();
        _metricsCollectorPtr->increaseSyntaxErrorQps();
        return;
    }
    
    _metricsCollectorPtr->requestParserEndTrigger();
    FetchSummaryClause *fetchSummaryClause =
        requestPtr->getFetchSummaryClause();
    if (!fetchSummaryClause) {
        ConfigClause *configClause = requestPtr->getConfigClause();
        configClause->setAllowLackSummaryConfig("no");
    }
    QrsProcessor::process(requestPtr, resultPtr);
}

QrsProcessorPtr RequestParserProcessor::clone() {
    QrsProcessorPtr ptr(new RequestParserProcessor(*this));
    return ptr;
}

} // namespace qrs
} // namespace isearch

