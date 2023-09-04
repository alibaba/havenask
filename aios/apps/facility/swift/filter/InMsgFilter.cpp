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
#include "swift/filter/InMsgFilter.h"

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include "swift/common/Common.h"
#include "swift/common/FieldGroupReader.h"

using namespace std;
using namespace swift::common;

namespace swift {
namespace filter {
AUTIL_LOG_SETUP(swift, InMsgFilter);

std::string InMsgFilter::OPTIONAL_FIELD_BEGIN_FLAG = "[";
std::string InMsgFilter::OPTIONAL_FIELD_END_FLAG = "]";

InMsgFilter::InMsgFilter(const std::string &fieldName, const std::string &setStr) {
    _optionalField = false;
    _descStr = setStr;
    parseFieldName(fieldName);
}

InMsgFilter::~InMsgFilter() {}

bool InMsgFilter::filterMsg(const FieldGroupReader &fieldGroupReader) const {
    const Field *field = fieldGroupReader.getField(_fieldName);
    if (field == NULL) {
        if (_optionalField) {
            return true;
        } else {
            return false;
        }
    }
    if (_fieldset.count(field->value) > 0) {
        return true;
    } else {
        return false;
    }
}
void InMsgFilter::parseFieldName(const std::string &fieldName) {
    if (fieldName.find(OPTIONAL_FIELD_BEGIN_FLAG) == 0 &&
        fieldName.rfind(OPTIONAL_FIELD_END_FLAG) == (fieldName.length() - OPTIONAL_FIELD_END_FLAG.length())) {
        _fieldName = fieldName.substr(OPTIONAL_FIELD_BEGIN_FLAG.length(),
                                      fieldName.length() - OPTIONAL_FIELD_BEGIN_FLAG.length() -
                                          OPTIONAL_FIELD_END_FLAG.length());
        _optionalField = true;
    } else {
        _fieldName = fieldName;
        _optionalField = false;
    }
}

bool InMsgFilter::init() {
    if (_fieldName.empty() || _descStr.empty()) {
        AUTIL_LOG(INFO, "InMsgFilter field Name [%s] or desc [%s] is empty.", _fieldName.c_str(), _descStr.c_str());
        return false;
    }
    autil::StringTokenizer st(
        _descStr, "|", autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    _fields.reserve(st.getNumTokens());
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        _fields.push_back(st[i]);
        _fieldset.insert(autil::StringView(_fields[i].data(), _fields[i].size()));
    }
    if (_fields.empty()) {
        AUTIL_LOG(INFO, "InMsgFilter value is empty,  [%s]", _descStr.c_str());
        return false;
    }
    return true;
}

} // namespace filter
} // namespace swift
