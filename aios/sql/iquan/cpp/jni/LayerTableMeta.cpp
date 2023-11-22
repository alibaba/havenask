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
using namespace autil::legacy::json;
using namespace autil::legacy;

namespace iquan {
AUTIL_LOG_SETUP(iquan, LayerTableMeta);

string LayerTableMeta::layerInfoTypeErrMsg(const string &layerTableName, const Layer &layer) const {
    string errMsg = "layerTable name : [" + layerTableName + "] layer : [" + layer.dbName + "."
                    + layer.tableName + "]'s layerInfo type is not JsonArray";
    return errMsg;
}

string LayerTableMeta::layerInfoRangeErrMsg(const string &layerTableName,
                                            const Layer &layer) const {
    string errMsg = "layerTable name : [" + layerTableName + "] layer : [" + layer.dbName + "."
                    + layer.tableName + "]'s layerInfo range field";
    return errMsg;
}

string LayerTableMeta::layerInfoFieldErrMsg(const std::string &layerTableName,
                                            const Layer &layer,
                                            const std::string &fieldName) const {
    string errMsg = "layerTable name : [" + layerTableName + "] layer: [" + layer.dbName + "."
                    + layer.tableName + "] has no field [" + fieldName + "]";
    return errMsg;
}

bool LayerTableMeta::isInt64(Any jsonAny, int64_t &value) {
    try {
        value = JsonNumberCast<int64_t>(jsonAny);
    } catch (const exception &e) { return false; }
    return true;
}

bool LayerTableMeta::processStringType(const LayerTableDef &layerTable,
                                       const string &layerTableName,
                                       const string &fieldName) {
    unordered_set<string> strSet;
    for (const Layer &layer : layerTable.layers) {
        if (layer.layerInfo.find(fieldName) == layer.layerInfo.end()) {
            AUTIL_LOG(ERROR,
                      "not find fieldName in layerInfo [%s]",
                      layerInfoFieldErrMsg(layerTableName, layer, fieldName).c_str());
            return false;
        }
        if (!IsJsonArray(layer.layerInfo.at(fieldName))) {
            AUTIL_LOG(ERROR,
                      "cast to vector<Any> failed due to %s, value is %s",
                      layerInfoTypeErrMsg(layerTableName, layer).c_str(),
                      ToString(layer.layerInfo.at(fieldName)).c_str());
            return false;
        }
        const vector<Any> &anyStrVec = AnyCast<vector<Any>>(layer.layerInfo.at(fieldName));
        for (auto &anyStr : anyStrVec) {
            if (!IsJsonString(anyStr)) {
                AUTIL_LOG(ERROR,
                          "cast to vectot<Any> failed due to in [%s]'s layerInfo should be string "
                          "but [%s]",
                          layerInfoTypeErrMsg(layerTableName, layer).c_str(),
                          ToString(anyStr).c_str());
                return false;
            }
            const string &str {AnyCast<string>(anyStr)};
            strSet.insert(str);
        }
    }
    _strValueMap[fieldName] = move(strSet);
    return true;
}

bool LayerTableMeta::processIntType(const LayerTableDef &layerTable,
                                    const string &layerTableName,
                                    const string &fieldName,
                                    const string &method) {
    vector<int64_t> vec;
    vec.push_back(numeric_limits<int64_t>::max());
    vec.push_back(numeric_limits<int64_t>::min());
    if (method == "range") {
        for (const Layer &layer : layerTable.layers) {
            if (layer.layerInfo.find(fieldName) == layer.layerInfo.end()) {
                AUTIL_LOG(ERROR,
                          "not find fieldName in layerInfo [%s]",
                          layerInfoFieldErrMsg(layerTableName, layer, fieldName).c_str());
                return false;
            }
            if (!IsJsonArray(layer.layerInfo.at(fieldName))) {
                AUTIL_LOG(ERROR,
                          "cast to vector<Any> failed due to %s, value is %s",
                          layerInfoTypeErrMsg(layerTableName, layer).c_str(),
                          ToString(layer.layerInfo.at(fieldName)).c_str());
                return false;
            }
            const vector<Any> &anyIntVecList = AnyCast<vector<Any>>(layer.layerInfo.at(fieldName));
            for (auto &anyIntVec : anyIntVecList) {
                if (!IsJsonArray(anyIntVec)) {
                    AUTIL_LOG(ERROR,
                              "cast to vector<Any> failed due to %s, value is %s",
                              layerInfoRangeErrMsg(layerTableName, layer).c_str(),
                              ToString(anyIntVec).c_str());
                    return false;
                }

                const vector<Any> &intVec = AnyCast<vector<Any>>(anyIntVec);
                if (2 != intVec.size()) {
                    AUTIL_LOG(ERROR, "[LayerTable layer] rangeValue should have 2 elements!");
                    return false;
                }

                int64_t from, to;
                if (!(isInt64(intVec[0], from) && isInt64(intVec[1], to))) {
                    AUTIL_LOG(ERROR,
                              "cast to int64_t failed. %s, layer range value is from %s to %s",
                              layerInfoRangeErrMsg(layerTableName, layer).c_str(),
                              ToString(intVec[0]).c_str(),
                              ToString(intVec[1]).c_str());
                    return false;
                }
                vec.push_back(from);
                vec.push_back(to);
            }
        }
    } else if (method == "grouping") {
        for (const Layer &layer : layerTable.layers) {
            if (layer.layerInfo.find(fieldName) == layer.layerInfo.end()) {
                AUTIL_LOG(ERROR,
                          "not find fieldName in layerInfo [%s]",
                          layerInfoFieldErrMsg(layerTableName, layer, fieldName).c_str());
                return false;
            }
            if (!IsJsonArray(layer.layerInfo.at(fieldName))) {
                AUTIL_LOG(ERROR,
                          "cast to vector<Any> failed due to %s, value is %s",
                          layerInfoTypeErrMsg(layerTableName, layer).c_str(),
                          ToString(layer.layerInfo.at(fieldName)).c_str());
                return false;
            }
            const vector<Any> &intVec = AnyCast<vector<Any>>(layer.layerInfo.at(fieldName));
            for (auto &anyValue : intVec) {
                int64_t intValue;
                if (!isInt64(anyValue, intValue)) {
                    AUTIL_LOG(ERROR,
                              "cast to int64_t failed. %s, value is %s",
                              layerInfoRangeErrMsg(layerTableName, layer).c_str(),
                              ToString(intVec[0]).c_str());
                    return false;
                }
                vec.push_back(intValue);
            }
        }
    }
    sort(vec.begin(), vec.end());
    _intValueMap[fieldName] = move(vec);
    return true;
}

bool LayerTableMeta::init(const LayerTableModel &layerTableModel) {
    _strValueMap.clear();
    _intValueMap.clear();

    const auto &layerTable = layerTableModel.tableContent;
    const auto &layerTableName = layerTableModel.layerTableName;
    for (const auto &layerFormat : layerTable.layerFormats) {
        const string &fieldName = layerFormat.fieldName;
        if (layerFormat.valueType == "string") {
            if (!processStringType(layerTable, layerTableName, fieldName)) {
                return false;
            }
        } else if (layerFormat.valueType == "int") {
            if (!processIntType(layerTable, layerTableName, fieldName, layerFormat.layerMethod)) {
                return false;
            }
        }
    }
    return true;
}

} // namespace iquan
