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

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "ha3/sql/ops/condition/ConditionVisitor.h"

namespace isearch {
namespace sql {
class AndCondition;
class LeafCondition;
class NotCondition;
class OrCondition;
} // namespace sql
} // namespace isearch

namespace isearch {
namespace sql {

class UpdateConditionVisitor : public ConditionVisitor {
public:
    UpdateConditionVisitor(const std::set<std::string> &fetchFields);
    virtual ~UpdateConditionVisitor();

public:
    void visitAndCondition(AndCondition *condition) override;
    void visitOrCondition(OrCondition *condition) override;
    void visitNotCondition(NotCondition *condition) override;
    void visitLeafCondition(LeafCondition *condition) override;
    bool fetchValues(std::vector<std::map<std::string, std::string>> &values);
protected:
    static bool hasSameFields(const std::map<std::string, std::string> &a,
                              const std::map<std::string, std::string> &b);
    static bool isIncludeFields(const std::map<std::string, std::string> &a,
                                const std::map<std::string, std::string> &b);
    static bool isSameFields(const std::map<std::string, std::string> &a,
                             const std::map<std::string, std::string> &b);
    bool parseKey(const autil::SimpleValue &condition);
    bool parseIn(const autil::SimpleValue &condition);
    bool parseEqual(const autil::SimpleValue &left,
                    const autil::SimpleValue &right);
    bool doParseEqual(const autil::SimpleValue &left,
                      const autil::SimpleValue &right);
    bool tryAddKeyValue(const autil::SimpleValue &attr,
                        const autil::SimpleValue &value);
    virtual bool checkFetchValues();
protected:
    std::set<std::string> _fetchFields;
    std::vector<std::map<std::string, std::string>> _values;
private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<UpdateConditionVisitor> UpdateConditionVisitorPtr;

} // namespace sql
} // namespace isearch
