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
#include <vector>

#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/filter/MsgFilter.h"

namespace swift {
namespace common {
class FieldGroupReader;
}
} // namespace swift

namespace swift {
namespace filter {

class ContainMsgFilter : public MsgFilter {
public:
    ContainMsgFilter(const std::string &fieldName, const std::string &setStr);
    ~ContainMsgFilter();

    const std::string &getFieldName() const { return _fieldName; }
    const std::string &getValueSearchStr() const { return _valuesSearchStr; }
    const std::vector<std::string> &getValuesSearch() const { return _valuesSearch; }

public:
    bool init() override;
    bool filterMsg(const common::FieldGroupReader &fieldGroupReader) const override;

private:
    std::string _valuesSearchStr;
    std::string _fieldName;
    std::vector<std::string> _valuesSearch;

private:
    AUTIL_LOG_DECLARE();
    friend class MsgFilterCreatorTest;
    friend class ContainMsgFilterTest;
};

SWIFT_TYPEDEF_PTR(ContainMsgFilter);

} // namespace filter
} // namespace swift
