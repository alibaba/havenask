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
#include "sql/ops/condition/ConditionParser.h"

#include <iosfwd>
#include <memory>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "autil/legacy/RapidJsonHelper.h"
#include "navi/common.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/ops/condition/Condition.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

using namespace std;
using namespace navi;
using namespace autil;

namespace sql {

ConditionParser::ConditionParser(autil::mem_pool::Pool *pool)
    : _pool(pool) {}

ConditionParser::~ConditionParser() {}

bool ConditionParser::parseCondition(const std::string &jsonStr, ConditionPtr &condition) {
    if (jsonStr.empty()) {
        return true;
    }
    AutilPoolAllocator *allocator = new AutilPoolAllocator(_pool);
    SimpleDocument *simpleDoc = new SimpleDocument(allocator);
    simpleDoc->Parse(jsonStr.c_str());
    if (simpleDoc->HasParseError()) {
        DELETE_AND_SET_NULL(simpleDoc);
        DELETE_AND_SET_NULL(allocator);
        SQL_LOG(WARN, "parse simple document failed, [%s]", jsonStr.c_str());
        return false;
    }
    ConditionPtr tmpCondition;
    if (!buildCondition(*simpleDoc, tmpCondition) || !validateCondition(tmpCondition)) {
        DELETE_AND_SET_NULL(simpleDoc);
        DELETE_AND_SET_NULL(allocator);
        SQL_LOG(WARN, "build & valid condition failed.");
        return false;
    }
    condition = tmpCondition;
    condition->setTopJsonDoc(allocator, simpleDoc);
    return true;
}

bool ConditionParser::buildCondition(const SimpleValue &simpleVal, ConditionPtr &condition) {
    if (!simpleVal.IsObject() || !simpleVal.HasMember(SQL_CONDITION_OPERATOR)
        || !simpleVal.HasMember(SQL_CONDITION_PARAMETER)) {
        condition.reset(Condition::createLeafCondition(simpleVal));
        return condition != nullptr;
    }
    string ops(simpleVal[SQL_CONDITION_OPERATOR].GetString());
    if (ops == SQL_AND_OP) {
        condition.reset(Condition::createCondition(AND_CONDITION));
    } else if (ops == SQL_OR_OP) {
        condition.reset(Condition::createCondition(OR_CONDITION));
    } else if (ops == SQL_NOT_OP) {
        condition.reset(Condition::createCondition(NOT_CONDITION));
    } else {
        condition.reset(Condition::createLeafCondition(simpleVal));
        return condition != nullptr;
    }
    const SimpleValue &opArray = simpleVal[SQL_CONDITION_PARAMETER];
    if (!opArray.IsArray()) {
        SQL_LOG(WARN, "cond param is not array");
        return false;
    }
    for (rapidjson::SizeType i = 0; i < opArray.Size(); i++) {
        const SimpleValue &param = opArray[i];
        if (param.IsObject()) {
            ConditionPtr child;
            if (!buildCondition(param, child)) {
                SQL_LOG(WARN,
                        "build cond [%s] failed",
                        RapidJsonHelper::SimpleValue2Str(param).c_str());
                return false;
            }
            condition->addCondition(child);
        } else if (param.IsString()) {
            ConditionPtr child;
            child.reset(new LeafCondition(param));
            condition->addCondition(child);
        } else {
            SQL_LOG(WARN, "unexpected param [%s]", RapidJsonHelper::SimpleValue2Str(param).c_str());
            return false;
        }
    }
    return true;
}

} // namespace sql
