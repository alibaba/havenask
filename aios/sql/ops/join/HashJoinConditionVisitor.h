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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/legacy/RapidJsonCommon.h"
#include "sql/ops/condition/ConditionVisitor.h"

namespace sql {
class AndCondition;
class LeafCondition;
class NotCondition;
class OrCondition;
} // namespace sql

namespace sql {

class HashJoinConditionVisitor : public ConditionVisitor {
public:
    HashJoinConditionVisitor(
        const std::map<std::string, std::pair<std::string, bool>> &output2InputMap);
    ~HashJoinConditionVisitor();

public:
    void visitAndCondition(AndCondition *condition) override;
    void visitOrCondition(OrCondition *condition) override;
    void visitNotCondition(NotCondition *condition) override;
    void visitLeafCondition(LeafCondition *condition) override;

public:
    void stealJoinColumns(std::vector<std::string> &left, std::vector<std::string> &right);

    std::vector<std::string> getLeftJoinColumns() {
        return _leftColumns;
    }
    std::vector<std::string> getRightJoinColumns() {
        return _rightColumns;
    }

private:
    bool
    parseJoinColumns(const autil::SimpleValue &condition, std::string &left, std::string &right);
    bool parseJoinColumn(const autil::SimpleValue &value, std::string &column, bool &hasAnyUdf);
    bool transOutputToInput(const std::string &outputFiled);

private:
    const std::map<std::string, std::pair<std::string, bool>> &_output2InputMap;
    std::vector<std::string> _leftColumns;
    std::vector<std::string> _rightColumns;
};

typedef std::shared_ptr<HashJoinConditionVisitor> HashJoinConditionVisitorPtr;
} // namespace sql
