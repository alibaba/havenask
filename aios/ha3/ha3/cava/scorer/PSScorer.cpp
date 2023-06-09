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
#include "ha3/cava/scorer/PSScorer.h"

#include "ha3/cava/scorer/PSWorker.h"

namespace isearch {
namespace compound_scorer {

AUTIL_LOG_SETUP(ha3, PSScorer);
using namespace isearch::common;
using namespace isearch::rank;
using namespace isearch::search;
using namespace isearch::config;
using namespace suez::turing;

static const std::string PSFIELD = "PS";

bool PSScorer::initWorker(std::vector<WorkerType> &result,
                          ScoringProvider *provider, bool isASC)
{
    auto traceRefer = provider->getTracerRefer();
    auto _tracer = provider->getRequestTracer();
    auto psRef = provider->requireAttributeWithoutType(PSFIELD);
    if (!psRef || psRef->getValueType().isMultiValue()) {
        return true;
    }
#define SET_PS_WORKERS(vt_type)                                         \
    case vt_type:                                                       \
    {                                                                   \
        typedef VariableTypeTraits<vt_type, false>::AttrItemType T;     \
        auto psRefTyped = dynamic_cast<const matchdoc::Reference<T>*>(psRef); \
        if (NULL == psRefTyped) {                                       \
            REQUEST_TRACE(DEBUG, "Get PS Field error");                 \
            return false;                                               \
        }                                                               \
        result.push_back(PSWorker<T>(psRefTyped, isASC, traceRefer));   \
        break;                                                          \
    }
    switch (psRef->getValueType().getBuiltinType()) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER(SET_PS_WORKERS);
    default:
        REQUEST_TRACE(WARN, "Unknow PS Type");
        break;
    }
#undef SET_PS_WORKERS
    return true;
}

} // namespace compound_scorer
} // namespace isearch
