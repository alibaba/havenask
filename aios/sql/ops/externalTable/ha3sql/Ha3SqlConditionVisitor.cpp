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
#include "sql/ops/externalTable/ha3sql/Ha3SqlConditionVisitor.h"

#include <assert.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>
#include <utility>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/RapidJsonHelper.h"
#include "sql/ops/condition/Condition.h"
#include "sql/ops/externalTable/ha3sql/Ha3SqlExprGeneratorVisitor.h"

using namespace std;
using namespace autil;

namespace sql {
AUTIL_LOG_SETUP(sql, Ha3SqlConditionVisitor);

#define CALL_AND_CHECK(expr)                                                                       \
    expr;                                                                                          \
    if (isError()) {                                                                               \
        return;                                                                                    \
    }

Ha3SqlConditionVisitor::Ha3SqlConditionVisitor(std::shared_ptr<autil::mem_pool::Pool> pool)
    : _dynamicParamsCollector(std::move(pool)) {}

Ha3SqlConditionVisitor::~Ha3SqlConditionVisitor() {}

void Ha3SqlConditionVisitor::visitAndCondition(AndCondition *condition) {
    assert(!isError());
    const auto &children = condition->getChildCondition();
    if (children.empty()) {
        return;
    }
    _ss << "(";
    for (size_t i = 0; i < children.size(); i++) {
        if (i > 0) {
            _ss << " AND ";
        }
        CALL_AND_CHECK(children[i]->accept(this));
    }
    _ss << ")";
}

void Ha3SqlConditionVisitor::visitOrCondition(OrCondition *condition) {
    assert(!isError());
    const auto &children = condition->getChildCondition();
    if (children.empty()) {
        return;
    }
    _ss << "(";
    for (size_t i = 0; i < children.size(); i++) {
        if (i > 0) {
            _ss << " OR ";
        }
        CALL_AND_CHECK(children[i]->accept(this));
    }
    _ss << ")";
}

void Ha3SqlConditionVisitor::visitNotCondition(NotCondition *condition) {
    assert(!isError());
    const auto &children = condition->getChildCondition();
    if (children.size() != 1) {
        setErrorInfo("[NOT] cond invalid, condition[%s]", condition->toString().c_str());
        return;
    }
    _ss << "(NOT ";
    CALL_AND_CHECK(children[0]->accept(this));
    _ss << ")";
}

void Ha3SqlConditionVisitor::visitLeafCondition(LeafCondition *condition) {
    assert(!isError());
    Ha3SqlExprGeneratorVisitor visitor(&_renameMap, &_dynamicParamsCollector);
    visitor.visit(condition->getCondition());
    if (visitor.isError()) {
        setErrorInfo("to expr string failed, error[%s], condition[%s]",
                     visitor.errorInfo().c_str(),
                     condition->toString().c_str());
        return;
    }
    auto exprStr = visitor.getExpr();
    SQL_LOG(TRACE1,
            "create leaf exprStr[%s] for condition[%s]",
            exprStr.c_str(),
            condition->toString().c_str());
    _ss << exprStr;
}

std::string Ha3SqlConditionVisitor::getConditionStr() const {
    auto str = _ss.str();
    for (const auto &pair : _renameMap) {
        autil::StringUtil::replaceAll(str, pair.first, pair.second);
    }
    return str;
}

std::string Ha3SqlConditionVisitor::getDynamicParamsStr() const {
    return autil::RapidJsonHelper::SimpleValue2Str(_dynamicParamsCollector.getSimpleValue());
}

} // namespace sql
