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
#include "sql/ops/condition/OutputFieldsVisitor.h"

#include <assert.h>
#include <cstddef>

#include "autil/TimeUtility.h"
#include "autil/legacy/RapidJsonHelper.h"
#include "sql/common/common.h"
#include "sql/ops/condition/ExprUtil.h"

using namespace std;
using namespace autil;

namespace sql {

OutputFieldsVisitor::OutputFieldsVisitor() {}

OutputFieldsVisitor::~OutputFieldsVisitor() {}

void OutputFieldsVisitor::visitParams(const SimpleValue &value) {
    const SimpleValue &params = value[SQL_CONDITION_PARAMETER];
    assert(params.IsArray());
    for (size_t i = 0; i < params.Size(); ++i) {
        visit(params[i]);
        if (isError()) {
            return;
        }
    }
}

void OutputFieldsVisitor::visitCastUdf(const SimpleValue &value) {
    visitParams(value);
}

void OutputFieldsVisitor::visitMultiCastUdf(const SimpleValue &value) {
    visitParams(value);
}

void OutputFieldsVisitor::visitNormalUdf(const SimpleValue &value) {
    visitParams(value);
}

void OutputFieldsVisitor::visitOtherOp(const SimpleValue &value) {
    visitParams(value);
}

void OutputFieldsVisitor::visitItemOp(const SimpleValue &value) {
    string itemName;
    string itemKey;
    if (!ExprUtil::parseItemVariable(value, itemName, itemKey)) {
        setErrorInfo("parse item variable failed: %s",
                     RapidJsonHelper::SimpleValue2Str(value).c_str());
        return;
    }
    _usedFieldsItemSet.insert(make_pair(itemName, itemKey));
}

void OutputFieldsVisitor::visitColumn(const SimpleValue &value) {
    _usedFieldsColumnSet.insert(value.GetString());
}

} // namespace sql
