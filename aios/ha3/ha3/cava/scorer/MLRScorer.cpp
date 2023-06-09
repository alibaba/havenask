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
#include "ha3/cava/scorer/MLRScorer.h"

#include "autil/StringUtil.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

#include "ha3/cava/scorer/MLRWorker.h"
#include "ha3/rank/ScoringProvider.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/Request.h"
namespace isearch {
namespace compound_scorer {

using namespace isearch::common;
using namespace isearch::rank;
using namespace isearch::search;
using namespace isearch::config;
using namespace isearch;
using namespace suez::turing;

AUTIL_LOG_SETUP(ha3, MLRScorer);

static const std::string MLRFIELD = "mlrfield";
static const std::string MLRSCORE = "mlrscore";
static const std::string MLRWEIGHT = "psweight";
static const std::string FIELD_SPERATOR = ",";

MLRScorer::MLRScorer(const std::string &sortFields)
{
    const auto &vecFields = autil::StringUtil::split(sortFields, FIELD_SPERATOR, true);
    _setSortField.insert(vecFields.begin(),vecFields.end());
}

bool MLRScorer::initWorker(std::vector<WorkerType> &result, ScoringProvider *provider, bool isASC)
{
    ConfigClause *configClause = provider->getRequest()->getConfigClause();
    if (configClause) {
        score_t MLRWeight = 1;
        const std::string &sMLRWeight = configClause->getKVPairValue(MLRWEIGHT);
        if (!sMLRWeight.empty()) {
            autil::StringUtil::fromString(sMLRWeight, MLRWeight);
        }
        std::string sMLRField = configClause->getKVPairValue(MLRFIELD);
        std::string sMLRValue = configClause->getKVPairValue(MLRSCORE);
        if (sMLRField.empty()) {
            sMLRField = "category";
        }
        if (!sMLRValue.empty()) {
            parse(sMLRField, sMLRValue, provider, result, MLRWeight);
        }
    }
    return true;
}

bool MLRScorer::parse(const std::string &sField, const std::string &sValue,
                      ScoringProvider *provider, std::vector<WorkerType>& result,
                      score_t MLRWeight)
{
    if (sField.empty() || sValue.empty()) {
        return true;
    }
    auto traceRefer = provider->getTracerRefer();
    auto _tracer = provider->getRequestTracer();
    const std::vector<std::string>& vecField =
        autil::StringUtil::split(sField, FIELDSEP, false);
    const std::vector<std::string>& vecValue =
        autil::StringUtil::split(sValue, FIELDSEP, false);
    if (vecField.size() != vecValue.size()) {
        REQUEST_TRACE(DEBUG, "mlrfield mlrvalue size not equal");
        return false;
    }
    for(size_t i = 0; i < vecField.size(); ++i) {
        auto attributeRefMLR = provider->requireAttributeWithoutType(vecField[i]);
        if (NULL == attributeRefMLR){
            REQUEST_TRACE(WARN, "MLRScorer : [field:%s] is NULL", vecField[i].c_str());
            continue;
        }
        if (attributeRefMLR->getValueType().isMultiValue()) {
            //the multi field is sort or not
            bool isSort = (_setSortField.find(vecField[i]) != _setSortField.end());
#define SET_MULTI_MLR_WORKERS(vt_type)                                  \
            case vt_type:{                                              \
                typedef VariableTypeTraits<vt_type, true>::AttrExprType T; \
                auto attr = dynamic_cast<const matchdoc::Reference<T>*>(attributeRefMLR); \
                if (attr == NULL) {                                     \
                    continue;                                           \
                }                                                       \
                if (isSort){                                            \
                    MLRWorkerSort<T> w(attr, MLRWeight, traceRefer);    \
                    if (!w.init(vecValue[i])) {                         \
                        continue;                                       \
                    }                                                   \
                    result.push_back(w);                                \
                } else {                                                \
                    MLRWorker<T> w(attr, MLRWeight, traceRefer);        \
                    if (!w.init(vecValue[i])) {                         \
                        continue;                                       \
                    }                                                   \
                    result.push_back(w);                                \
                }                                                       \
                break;                                                  \
            }
            switch (attributeRefMLR->getValueType().getBuiltinType()) {
                INTEGER_VARIABLE_TYPE_MACRO_HELPER(SET_MULTI_MLR_WORKERS)
            default:
                REQUEST_TRACE(WARN, "Unknow [field:%s] Type", vecField[i].c_str());
                return false;
            }
#undef SET_MULTI_MLR_WORKERS
        } else {
#define SET_SINGLE_MLR_WORKERS(vt_type)                                 \
            case vt_type:{                                              \
                typedef VariableTypeTraits<vt_type, false>::AttrItemType T; \
                auto attr = dynamic_cast<const matchdoc::Reference<T>*>(attributeRefMLR); \
                if (attr == NULL) {                                     \
                    continue;                                           \
                }                                                       \
                MLRWorker<T> w(attr, MLRWeight, traceRefer);            \
                if (!w.init(vecValue[i])) {                             \
                    continue;                                           \
                }                                                       \
                result.push_back(w);                                    \
                break;                                                  \
            }
            switch (attributeRefMLR->getValueType().getBuiltinType()) {
                INTEGER_VARIABLE_TYPE_MACRO_HELPER(SET_SINGLE_MLR_WORKERS);
            default:
                REQUEST_TRACE(WARN, "Unknow [field:%s] Type", vecField[i].c_str());
                return false;
            }
#undef SET_SINGLE_MLR_WORKERS
        }
    }
    return true;
}

} // namespace compound_scorer
} // namespace isearch
