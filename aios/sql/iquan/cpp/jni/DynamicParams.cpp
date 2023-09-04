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
#include "iquan/jni/DynamicParams.h"

#include "iquan/common/IquanException.h"

using namespace autil::legacy::json;
using namespace autil::legacy;
using namespace std;

namespace iquan {

AUTIL_LOG_SETUP(iquan, DynamicParams);

DynamicParams::DynamicParams() {}

DynamicParams::~DynamicParams() {}

const autil::legacy::Any &DynamicParams::at(std::size_t idx, std::size_t sql_idx) const {
    if (sql_idx >= _array.size() || idx >= _array[sql_idx].size()) {
        throw IquanException("dynamic params idx out of range", IQUAN_FAIL);
    }
    if (IsJsonMap(_array[sql_idx][idx])) {
        JsonMap keyMap = autil::legacy::AnyCast<JsonMap>(_array[sql_idx][idx]);
        auto iter = keyMap.find("key");
        if (iter != keyMap.end()) {
            string key = autil::legacy::AnyCast<string>(iter->second);
            if (sql_idx < _map.size() && _map[sql_idx].find(key) != _map[sql_idx].end()) {
                return _map[sql_idx].at(key);
            } else {
                AUTIL_LOG(WARN, "dynamic params key[%s] not found", key.c_str());
                return _array[sql_idx][idx];
            }
        } else {
            return _array[sql_idx][idx];
        }
    } else {
        return _array[sql_idx][idx];
    }
}

const autil::legacy::Any &DynamicParams::at(std::string key, std::size_t sql_idx) const {
    if (sql_idx < _map.size()) {
        auto iter = _map[sql_idx].find(key);
        if (iter != _map[sql_idx].end()) {
            return iter->second;
        }
    }
    throw IquanException("key [" + key + "] not found in dynamic params", IQUAN_FAIL);
}

bool DynamicParams::addKVParams(const string &key, const Any &val, size_t sql_idx) {
    if (sql_idx >= _map.size()) {
        AUTIL_LOG(ERROR, "sql_idx[%lu] out of range when add kv params", sql_idx);
        return false;
    } else if (_map[sql_idx].find(key) != _map[sql_idx].end()) {
        AUTIL_LOG(ERROR, "add duplicate key[%s] when add kv params", key.c_str());
        return false;
    } else {
        _map[sql_idx][key] = val;
        return true;
    }
}

bool DynamicParams::findParamWithKey(const string &key, std::size_t sql_idx) const {
    if (sql_idx < _map.size() && _map[sql_idx].find(key) != _map[sql_idx].end()) {
        return true;
    } else {
        return false;
    }
}

bool DynamicParams::empty(size_t sql_idx) const {
    if ((sql_idx >= _array.size() || _array[sql_idx].empty())
        && (sql_idx >= _map.size() || _map[sql_idx].empty())) {
        return true;
    } else {
        return false;
    }
}

bool DynamicParams::isHintParamsEmpty() const {
    return _hint.empty();
}

bool DynamicParams::findHintParam(const string &key) const {
    return _hint.find(key) != _hint.end();
}

string DynamicParams::getHintParam(const string &key) const {
    if (findHintParam(key)) {
        return _hint.at(key);
    } else {
        AUTIL_LOG(WARN, "hint param [%s] not found", key.c_str());
        return "";
    }
}

void DynamicParams::reserveOneSqlParams() {
    _map.emplace_back(map<string, Any> {});
}

} // namespace iquan
