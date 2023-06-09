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
#include "ha3/cava/scorer/WeightScorer.h"

#include "autil/StringUtil.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

#include "ha3/cava/scorer/WeightWorker.h"
#include "ha3/common/Request.h"
#include "ha3/common/ConfigClause.h"

namespace isearch {
namespace compound_scorer {

using namespace isearch::common;
using namespace isearch::rank;
using namespace isearch::search;
using namespace isearch::config;
using namespace isearch;
using namespace suez::turing;

AUTIL_LOG_SETUP(ha3, WeightScorer);

static const std::string RANKFIELD = "rankfield";
static const std::string RANKVALUE = "rankvalue";
static const int FACTOR = INT32_MAX / 102;
static const int64_t HIGH_FACTOR = (int64_t) INT32_MAX * INT8_MAX / 102;

bool WeightScorer::initWorker(std::vector<WorkerType>& result,
                              ScoringProvider *provider, bool isASC)
{
    ConfigClause *configClause = provider->getRequest()->getConfigClause();
    if (configClause){
        const std::string& sRankField = configClause->getKVPairValue(RANKFIELD);
        const std::string& sRankValue = configClause->getKVPairValue(RANKVALUE);
        if (!sRankField.empty() && !sRankValue.empty()) {
            parse(sRankField, sRankValue, provider, isASC, result);
        }
    }
    return true;
}

bool WeightScorer::parse(const std::string &sField, const std::string &sValue,
         ScoringProvider *provider,bool isASC,std::vector<WorkerType>& result)
{
    if (sField.empty() || sValue.empty()) {
        return true;
    }
    auto traceRefer = provider->getTracerRefer();
    auto _tracer = provider->getRequestTracer();
    std::vector<std::string> vecField = autil::StringUtil::split(sField, FIELDSEP, false);
    std::vector<std::string> vecValue = autil::StringUtil::split(sValue, FIELDSEP, false);
    if (vecField.size() != vecValue.size()) {
        REQUEST_TRACE(DEBUG, "rankfield rankvalue size not equal");
        return false;
    }
    for (size_t i = 0; i < vecField.size(); ++i) {
        auto ref = provider->requireAttributeWithoutType(vecField[i]);
        if (!ref || ref->getValueType().isMultiValue()) {
            REQUEST_TRACE(WARN, "WeightScorer : [field:%s] is NULL or MultiValue field",
                          vecField[i].c_str());
            continue;
        }
#define SET_SINGLE_MLR_WORKERS(vt_type)                                 \
        case vt_type: {                                                 \
            typedef VariableTypeTraits<vt_type, false>::AttrItemType T; \
            auto attr = dynamic_cast<const matchdoc::Reference<T>*>(ref); \
            if (attr == NULL) {                                         \
                continue;                                               \
            }                                                           \
            int64_t factor = FACTOR;                                    \
            if (vecField.size() > 1 && i==0) {                          \
                factor = HIGH_FACTOR;                                   \
            }                                                           \
            WeightWorker<T> w(attr, isASC, factor, traceRefer);         \
            if (!w.init(vecValue[i])){                                  \
                continue;                                               \
            }                                                           \
            result.push_back(w);                                        \
            break;                                                      \
        }

        switch(ref->getValueType().getBuiltinType()){
            INTEGER_VARIABLE_TYPE_MACRO_HELPER(SET_SINGLE_MLR_WORKERS);
        default:
            REQUEST_TRACE(WARN, "Unknow [field:%s] Type", vecField[i].c_str());
            return false;
        }
#undef SET_SINGLE_MLR_WORKERS
    }
    return true;
}

} // namespace compound_scorer
} // namespace isearch
