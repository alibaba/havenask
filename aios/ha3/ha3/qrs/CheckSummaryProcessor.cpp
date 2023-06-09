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
#include "ha3/qrs/CheckSummaryProcessor.h"

#include <assert.h>
#include <memory>

#include "ha3/common/ClauseBase.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/Hits.h"
#include "ha3/common/Request.h"
#include "ha3/qrs/QrsProcessor.h"
#include "autil/Log.h"

using namespace isearch::common;
namespace isearch {
namespace qrs {
AUTIL_LOG_SETUP(ha3, CheckSummaryProcessor);

CheckSummaryProcessor::CheckSummaryProcessor() { 
}

CheckSummaryProcessor::~CheckSummaryProcessor() { 
}

QrsProcessorPtr CheckSummaryProcessor::clone() {
    QrsProcessorPtr ptr(new CheckSummaryProcessor(*this));
    return ptr;
}

void CheckSummaryProcessor::process(RequestPtr &requestPtr,
                                    ResultPtr &resultPtr)
{
    QrsProcessor::process(requestPtr, resultPtr);
    ConfigClause *configClause = requestPtr->getConfigClause();
    assert(configClause);
    if (configClause->isNoSummary()) {
        Hits *hits = resultPtr->getHits();
        if (hits) {
            hits->setNoSummary(true);
            hits->setIndependentQuery(true);
        }
    } else {
        QrsProcessor::fillSummary(requestPtr, resultPtr);
    }
}

} // namespace qrs
} // namespace isearch

