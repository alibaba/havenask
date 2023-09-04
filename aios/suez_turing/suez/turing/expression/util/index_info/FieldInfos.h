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
#pragma once

#include <map>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "indexlib/indexlib.h"

namespace suez {
namespace turing {

class FieldInfo {
public:
    FieldInfo();
    FieldInfo(const std::string &fieldName, FieldType fieldType, bool isMulti = false) {
        this->fieldName = fieldName;
        this->fieldType = fieldType;
        this->isMultiValue = isMulti;
    }
    ~FieldInfo();

public:
    bool operator==(const FieldInfo &fieldInfo) const;
    bool operator!=(const FieldInfo &fieldInfo) const { return !(*this == fieldInfo); }

public:
    std::string fieldName;
    std::string analyzerName;
    FieldType fieldType;
    bool isMultiValue;

private:
    AUTIL_LOG_DECLARE();
};

class FieldInfos {
public:
    FieldInfos();
    ~FieldInfos();
    FieldInfos(const FieldInfos &fieldInfos);

public:
    void addFieldInfo(FieldInfo *fieldInfo);
    FieldInfo *getFieldInfo(const char *fieldName) const;
    void getAllFieldNames(std::vector<std::string> &fieldNames) const;
    uint32_t getFieldCount() const { return (uint32_t)_fields.size(); }
    void stealFieldInfos(std::vector<FieldInfo *> &fieldInfos);

private:
    void reset();

private:
    typedef std::map<std::string, FieldInfo *> FieldMap;
    FieldMap _fields;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
