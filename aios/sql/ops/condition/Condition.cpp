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
#include "sql/ops/condition/Condition.h"

#include <assert.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "autil/legacy/RapidJsonHelper.h"
#include "sql/ops/condition/ConditionVisitor.h"

using namespace std;
using namespace autil;

namespace sql {

Condition::Condition()
    : _allocator(NULL)
    , _topDoc(NULL)

{}

Condition::~Condition() {
    DELETE_AND_SET_NULL(_topDoc);
    DELETE_AND_SET_NULL(_allocator);
}

void Condition::setTopJsonDoc(autil::AutilPoolAllocator *allocator, autil::SimpleDocument *topDoc) {
    if (_topDoc) {
        DELETE_AND_SET_NULL(_topDoc);
    }
    _topDoc = topDoc;
    if (_allocator) {
        DELETE_AND_SET_NULL(_allocator);
    }
    _allocator = allocator;
}

LeafCondition::LeafCondition(const autil::SimpleValue &simpleVal)
    : _condition(simpleVal) {}

Condition *Condition::createCondition(ConditionType type) {
    Condition *scanCond = NULL;
    switch (type) {
    case AND_CONDITION:
        scanCond = new AndCondition();
        break;
    case OR_CONDITION:
        scanCond = new OrCondition();
        break;
    case NOT_CONDITION:
        scanCond = new NotCondition();
        break;
    default:
        assert(false);
    }
    return scanCond;
}

Condition *Condition::createLeafCondition(const autil::SimpleValue &simpleVal) {
    return new LeafCondition(simpleVal);
}

void AndCondition::accept(ConditionVisitor *visitor) {
    visitor->visitAndCondition(this);
}

void OrCondition::accept(ConditionVisitor *visitor) {
    visitor->visitOrCondition(this);
}

void NotCondition::accept(ConditionVisitor *visitor) {
    visitor->visitNotCondition(this);
}

void LeafCondition::accept(ConditionVisitor *visitor) {
    visitor->visitLeafCondition(this);
}

string AndCondition::toString() {
    if (_children.size() == 0) {
        return "";
    } else if (_children.size() == 1) {
        return _children[0]->toString();
    }
    string conditionStr;
    for (size_t i = 0; i < _children.size() - 1; i++) {
        conditionStr += _children[i]->toString() + " AND ";
    }
    conditionStr += _children[_children.size() - 1]->toString();
    conditionStr = "(" + conditionStr + ")";
    return conditionStr;
}

string OrCondition::toString() {
    if (_children.size() == 0) {
        return "";
    } else if (_children.size() == 1) {
        return _children[0]->toString();
    }
    string conditionStr;
    for (size_t i = 0; i < _children.size() - 1; i++) {
        conditionStr += _children[i]->toString() + " OR ";
    }
    conditionStr += _children[_children.size() - 1]->toString();
    conditionStr = "(" + conditionStr + ")";
    return conditionStr;
}

string NotCondition::toString() {
    if (_children.size() == 0) {
        return "";
    }
    string conditionStr = _children[0]->toString();
    conditionStr = "NOT " + conditionStr;
    return conditionStr;
}

string LeafCondition::toString() {
    return RapidJsonHelper::SimpleValue2Str(_condition);
}

} // namespace sql
