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

#include <string>
#include <unordered_set>
#include <vector>

#include "autil/Log.h"
#include "autil/Span.h"
#include "swift/common/Common.h"
#include "swift/filter/HashUtil.h" // IWYU pragma: keep
#include "swift/filter/MsgFilter.h"

namespace swift {
namespace common {

class FieldGroupReader;
} // namespace common
} // namespace swift

namespace swift {
namespace filter {

class InMsgFilter : public MsgFilter {
public:
    typedef std::unordered_set<autil::StringView, SimpleHash> FieldsSet;
    static std::string OPTIONAL_FIELD_BEGIN_FLAG;
    static std::string OPTIONAL_FIELD_END_FLAG;

public:
    InMsgFilter(const std::string &fieldName, const std::string &setStr);
    ~InMsgFilter();

private:
    InMsgFilter(const InMsgFilter &);
    InMsgFilter &operator=(const InMsgFilter &);

public:
    bool init() override;
    bool filterMsg(const common::FieldGroupReader &fieldGroupReader) const override;
    bool isOptionalField() { return _optionalField; }

public:
    // for test
    const FieldsSet &getFilterSet() const { return _fieldset; }
    const std::string &getFieldName() const { return _fieldName; }

private:
    void parseFieldName(const std::string &fieldName);

private:
    FieldsSet _fieldset;
    std::vector<std::string> _fields;
    std::string _fieldName;
    std::string _descStr;
    bool _optionalField;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(InMsgFilter);

} // namespace filter
} // namespace swift
