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
#include "swift/filter/ContainMsgFilter.h"

#include <algorithm>
#include <cstddef>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/Span.h"
#include "autil/StringTokenizer.h"
#include "swift/common/Common.h"
#include "swift/common/FieldGroupReader.h"

using namespace std;
using namespace swift::common;

namespace swift {
namespace filter {
AUTIL_LOG_SETUP(swift, ContainMsgFilter);

ContainMsgFilter::ContainMsgFilter(const std::string &fieldName, const std::string &valuesSearchStr) {
    _fieldName = fieldName;
    _valuesSearchStr = valuesSearchStr;
}

ContainMsgFilter::~ContainMsgFilter() {}

bool ContainMsgFilter::filterMsg(const FieldGroupReader &fieldGroupReader) const {
    const Field *field = fieldGroupReader.getField(_fieldName);
    bool isContain = false;
    if (field == NULL) {
        return isContain;
    }
    for (size_t i = 0; i < _valuesSearch.size(); i++) {
        string::size_type pos = field->value.find(_valuesSearch[i]);
        if (pos != string::npos) {
            isContain = true;
            break;
        }
    }
    return isContain;
}

bool ContainMsgFilter::init() {
    if (_fieldName.empty() || _valuesSearchStr.empty()) {
        AUTIL_LOG(INFO,
                  "ContainMsgFilter fieldName [%s] or valueSearchStr [%s] is empty.",
                  _fieldName.c_str(),
                  _valuesSearchStr.c_str());
        return false;
    }
    if (!_valuesSearchStr.empty()) {
        autil::StringTokenizer st(
            _valuesSearchStr, "|", autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
        for (size_t i = 0; i < st.getNumTokens(); ++i) {
            _valuesSearch.push_back(st[i]);
        }
    }
    if (_valuesSearch.empty()) {
        AUTIL_LOG(INFO, "ContainMsgFilter value is empty,  [%s]", _valuesSearchStr.c_str());
        return false;
    }
    return true;
}

} // namespace filter
} // namespace swift
