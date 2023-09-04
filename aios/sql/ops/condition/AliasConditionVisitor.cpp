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
#include "sql/ops/condition/AliasConditionVisitor.h"

#include <assert.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>
#include <vector>

#include "autil/TimeUtility.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "sql/ops/condition/Condition.h"
#include "sql/ops/condition/ExprUtil.h"
#include "suez/turing/expression/common.h"

using namespace std;
using namespace autil;
using namespace suez::turing;

namespace sql {

AliasConditionVisitor::AliasConditionVisitor() {}

AliasConditionVisitor::~AliasConditionVisitor() {}

void AliasConditionVisitor::visitAndCondition(AndCondition *condition) {
    CHECK_ERROR_AND_RETRUN();
    const vector<ConditionPtr> &children = condition->getChildCondition();
    for (size_t i = 0; i < children.size(); i++) {
        children[i]->accept(this);
    }
}

void AliasConditionVisitor::visitOrCondition(OrCondition *condition) {
    CHECK_ERROR_AND_RETRUN();
    const vector<ConditionPtr> &children = condition->getChildCondition();
    for (size_t i = 0; i < children.size(); i++) {
        children[i]->accept(this);
    }
}

void AliasConditionVisitor::visitNotCondition(NotCondition *condition) {
    CHECK_ERROR_AND_RETRUN();
    const vector<ConditionPtr> &children = condition->getChildCondition();
    assert(children.size() == 1);
    children[0]->accept(this);
}

void AliasConditionVisitor::visitLeafCondition(LeafCondition *condition) {
    CHECK_ERROR_AND_RETRUN();
    const SimpleValue &leafCondition = condition->getCondition();
    bool hasError = isError();
    string errorInfo;
    string exprStr = ExprUtil::toExprString(leafCondition, hasError, errorInfo, _aliasMap);
    if (hasError) {
        setErrorInfo(errorInfo);
        return;
    }
}

} // namespace sql
