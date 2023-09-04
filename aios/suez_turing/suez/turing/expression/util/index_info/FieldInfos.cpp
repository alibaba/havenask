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
#include "suez/turing/expression/util/FieldInfos.h"

#include <cstddef>
#include <utility>

#include "alog/Logger.h"

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, FieldInfos);
AUTIL_LOG_SETUP(expression, FieldInfo);

FieldInfo::FieldInfo() {
    fieldType = ft_unknown;
    isMultiValue = false;
}

FieldInfo::~FieldInfo() {}

bool FieldInfo::operator==(const FieldInfo &fieldInfo) const {
    return (this->fieldName == fieldInfo.fieldName && this->analyzerName == fieldInfo.analyzerName &&
            this->fieldType == fieldInfo.fieldType && this->isMultiValue == fieldInfo.isMultiValue);
}

//////////////////////////////////////////////////////////////

FieldInfos::FieldInfos() {}

FieldInfos::FieldInfos(const FieldInfos &fieldInfos) {
    reset();
    for (FieldMap::const_iterator iter = fieldInfos._fields.begin(); iter != fieldInfos._fields.end(); iter++) {
        addFieldInfo(new FieldInfo(*(iter->second)));
    }
}

FieldInfos::~FieldInfos() { reset(); }

void FieldInfos::addFieldInfo(FieldInfo *fieldInfo) {
    FieldMap::iterator it = _fields.find(fieldInfo->fieldName);
    if (it != _fields.end()) {
        AUTIL_LOG(WARN, "Duplicated field: %s", fieldInfo->fieldName.c_str());
        delete fieldInfo;
        return;
    }
    _fields.insert(make_pair(fieldInfo->fieldName, fieldInfo));
}

FieldInfo *FieldInfos::getFieldInfo(const char *fieldName) const {
    FieldMap::const_iterator it = _fields.find(fieldName);
    if (it == _fields.end()) {
        AUTIL_LOG(TRACE2, "NOT find the fieldname: %s", fieldName);
        return NULL;
    }
    return it->second;
}

void FieldInfos::getAllFieldNames(std::vector<std::string> &fieldNames) const {
    fieldNames.clear();
    FieldMap::const_iterator it = _fields.begin();
    for (; it != _fields.end(); it++) {
        fieldNames.push_back(it->first);
    }
}

void FieldInfos::reset() {
    for (FieldMap::iterator it = _fields.begin(); it != _fields.end(); it++) {
        delete it->second;
    }
    _fields.clear();
}

void FieldInfos::stealFieldInfos(vector<FieldInfo *> &fieldInfos) {
    FieldMap::const_iterator it = _fields.begin();
    for (; it != _fields.end(); it++) {
        fieldInfos.push_back(it->second);
    }
    _fields.clear();
}

} // namespace turing
} // namespace suez
