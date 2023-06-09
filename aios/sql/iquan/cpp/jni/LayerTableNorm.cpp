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
#include <sstream>

#include "iquan/jni/LayerTableNorm.h"

using namespace std;

namespace iquan {
namespace dynamicParamCache {
AUTIL_LOG_SETUP(dynamicParamCache, LayerTableNorm);

const string STR_NOT_EXIST = "****NOT$EXIST****";

LayerTableNorm::LayerTableNorm(const vector<LayerTablePlanMeta> &metas)
    : planMetas(metas)
{};

bool LayerTableNorm::normalize(const unordered_map<string, LayerTableMetaPtr> &layerTableMetaMap,
                               const vector<autil::legacy::Any> &params,
                               string &result) const 
{
    stringstream out;
    for (const auto &planMeta : planMetas) {
        string normedStr;
        if (!doNorm(layerTableMetaMap, planMeta, params, normedStr)) {
            return false;
        }
        out << normedStr << "#";
    }
    result = out.str();
    AUTIL_LOG(DEBUG, "normalized params : %s", result.c_str());
    return true;
};

bool LayerTableNorm::doNorm(const unordered_map<string, LayerTableMetaPtr> &layerTableMetaMap,
                            const LayerTablePlanMeta &planMeta,
                            const vector<autil::legacy::Any> &params,
                            string &result) const
{
    const string &layerTableName = planMeta.layerTableName;
    const string &fieldName = planMeta.fieldName;
    auto op = planMeta.op;
    auto isRev = planMeta.isRev;
    int paramId = planMeta.dynamicParamsIndex;
    auto &value = params[paramId];

    auto iter = layerTableMetaMap.find(layerTableName);
    if (iter == layerTableMetaMap.end()) {
        return false;
    }
    LayerTableMetaPtr layerTableMeta = iter->second;
    const auto &strValueMap = layerTableMeta->getStrValueMap();
    auto strValueSetIter = strValueMap.find(fieldName);
    if (strValueSetIter != strValueMap.end()) {
        if (op != CompareOp::EQUALS) {
            return false;
        }
        const string &strVal = autil::legacy::AnyCast<string>(value);
        const auto &strSet = strValueSetIter->second;
        result = strSet.find(strVal) != strSet.end() ? strVal : STR_NOT_EXIST;
        return true;
    }

    const auto &intValueMap = layerTableMeta->getIntValueMap();
    auto intValueVecIter = intValueMap.find(fieldName);
    if (intValueVecIter != intValueMap.end()) {
        const auto &intVec = intValueVecIter->second;
        int64_t intValue = autil::legacy::AnyNumberCast<int64_t>(value);

        switch(op) {
            case EQUALS: {
                auto iter = lower_bound(intVec.begin(), intVec.end(), intValue);
                int ret =  *iter == intValue ? intValue : *(--iter) + 1;
                result = to_string(ret);
                return true;
            }
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL: {
                bool greater = (op == GREATER_THAN || op == GREATER_THAN_OR_EQUAL);
                if (greater == isRev) {
                    auto iter = upper_bound(intVec.begin(), intVec.end(), intValue);
                    result = to_string(*(--iter));
                    return true;
                } else {
                    auto iter = lower_bound(intVec.begin(), intVec.end(), intValue);
                    result= to_string(*iter);
                    return true;
                }
            }
        }
    }
    return false;
}

} // namespace dynamicParamCache
} // namespce iquan
