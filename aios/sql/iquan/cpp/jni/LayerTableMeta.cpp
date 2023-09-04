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
#include "iquan/jni/LayerTableMeta.h"

#include <algorithm>
#include <limits>

using namespace std;
namespace iquan {
AUTIL_LOG_SETUP(iquan, LayerTableMeta);

LayerTableMeta::LayerTableMeta(const LayerTableDef &layerTable) {
    for (const auto &layerFormat : layerTable.layerFormats) {
        const string fieldName = layerFormat.fieldName;
        if (layerFormat.valueType == "string") {
            unordered_set<string> strSet;
            for (const Layer &layer : layerTable.layers) {
                const vector<autil::legacy::Any> &anyStrVec
                    = autil::legacy::AnyCast<vector<autil::legacy::Any>>(
                        layer.layerInfo.at(fieldName));
                for (auto &anyStr : anyStrVec) {
                    const string &str {autil::legacy::AnyCast<string>(anyStr)};
                    strSet.insert(str);
                }
            }
            strValueMap[fieldName] = move(strSet);
        } else if (layerFormat.valueType == "int") {
            vector<int64_t> vec;
            vec.push_back(numeric_limits<int64_t>::max());
            vec.push_back(numeric_limits<int64_t>::min());
            if (layerFormat.layerMethod == "range") {
                for (const Layer &layer : layerTable.layers) {
                    const vector<autil::legacy::Any> &anyIntVecList
                        = autil::legacy::AnyCast<vector<autil::legacy::Any>>(
                            layer.layerInfo.at(fieldName));
                    for (auto &anyIntVec : anyIntVecList) {
                        const vector<autil::legacy::Any> &intVec
                            = autil::legacy::AnyCast<vector<autil::legacy::Any>>(anyIntVec);
                        if (2 != intVec.size()) {
                            AUTIL_LOG(ERROR,
                                      "[LayerTable layer] rangeValue should have 2 elements!");
                        }
                        vec.push_back(autil::legacy::AnyNumberCast<int64_t>(intVec[0]));
                        vec.push_back(autil::legacy::AnyNumberCast<int64_t>(intVec[1]));
                    }
                }
            } else if (layerFormat.layerMethod == "grouping") {
                for (const Layer &layer : layerTable.layers) {
                    const vector<autil::legacy::Any> &intVec
                        = autil::legacy::AnyCast<vector<autil::legacy::Any>>(
                            layer.layerInfo.at(fieldName));
                    for (auto &anyValue : intVec) {
                        int64_t intValue = autil::legacy::AnyNumberCast<int64_t>(anyValue);
                        vec.push_back(intValue);
                    }
                }
            }
            sort(vec.begin(), vec.end());
            intValueMap[fieldName] = move(vec);
        }
    }
}

} // namespace iquan
