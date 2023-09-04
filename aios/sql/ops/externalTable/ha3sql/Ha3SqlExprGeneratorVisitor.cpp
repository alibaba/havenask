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
#include "sql/ops/externalTable/ha3sql/Ha3SqlExprGeneratorVisitor.h"

#include <assert.h>
#include <cstddef>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include <unordered_map>
#include <utility>

#include "autil/StringUtil.h"
#include "sql/common/common.h"
#include "sql/ops/condition/ExprUtil.h"
#include "sql/ops/condition/SqlJsonUtil.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

using namespace std;

namespace sql {

#define VISIT_AND_CHECK(value)                                                                     \
    visit((value));                                                                                \
    if (isError()) {                                                                               \
        return;                                                                                    \
    }

Ha3SqlDynamicParamsCollector::Ha3SqlDynamicParamsCollector(
    std::shared_ptr<autil::mem_pool::Pool> pool)
    : _pool(std::move(pool))
    , _allocator(_pool.get())
    , _document(&_allocator) {
    _document.SetArray();
    _document.PushBack(autil::SimpleValue(rapidjson::kArrayType), _allocator);
}

Ha3SqlDynamicParamsCollector::~Ha3SqlDynamicParamsCollector() {}

void Ha3SqlDynamicParamsCollector::emplace(const autil::SimpleValue &input) {
    autil::SimpleValue val(input, _allocator);
    _document[0].PushBack(val, _allocator);
}

const autil::SimpleValue &Ha3SqlDynamicParamsCollector::getSimpleValue() const {
    return _document;
}

Ha3SqlExprGeneratorVisitor::Ha3SqlExprGeneratorVisitor(
    ExprGenerateVisitor::VisitorMap *renameMap,
    Ha3SqlDynamicParamsCollector *dynamicParamsCollector)
    : ExprGenerateVisitor(renameMap)
    , _dynamicParamsCollector(dynamicParamsCollector) {}

Ha3SqlExprGeneratorVisitor::~Ha3SqlExprGeneratorVisitor() {}

void Ha3SqlExprGeneratorVisitor::visitInt64(const autil::SimpleValue &value) {
    _expr = "?";
    _dynamicParamsCollector->emplace(value);
}

void Ha3SqlExprGeneratorVisitor::visitDouble(const autil::SimpleValue &value) {
    _expr = "?";
    _dynamicParamsCollector->emplace(value);
}

void Ha3SqlExprGeneratorVisitor::visitBool(const autil::SimpleValue &value) {
    _expr = "?";
    _dynamicParamsCollector->emplace(value);
}

void Ha3SqlExprGeneratorVisitor::visitString(const autil::SimpleValue &value) {
    _expr = "?";
    _dynamicParamsCollector->emplace(value);
}

void Ha3SqlExprGeneratorVisitor::parseWithOp(const autil::SimpleValue &value,
                                             const std::string &op) {
    const autil::SimpleValue &param = value[SQL_CONDITION_PARAMETER];
    VISIT_AND_CHECK(param[0]);
    std::string tmpExpr = _expr + " " + op + " (";
    for (size_t i = 1; i < param.Size(); i++) {
        tmpExpr += i != 1 ? "," : "";
        VISIT_AND_CHECK(param[i]);
        tmpExpr += _expr;
    }
    _expr = tmpExpr + ")";
}

void Ha3SqlExprGeneratorVisitor::visitInOp(const autil::SimpleValue &value) {
    parseWithOp(value, "in");
}

void Ha3SqlExprGeneratorVisitor::visitNotInOp(const autil::SimpleValue &value) {
    parseWithOp(value, "notin");
}

void Ha3SqlExprGeneratorVisitor::visitColumn(const autil::SimpleValue &value) {
    string name = SqlJsonUtil::getColumnName(value);
    wrapBacktick(name);
    string newName = name;
    ExprUtil::convertColumnName(newName);
    assert(newName != name);
    auto iter = _renameMapPtr->find(newName);
    if (iter != _renameMapPtr->end()) {
        if (iter->second != name) {
            setErrorInfo("rename map key[%s] conflict: [%s] vs [%s]",
                         newName.c_str(),
                         iter->second.c_str(),
                         name.c_str());
            return;
        }
    }
    _renameMapPtr->insert(make_pair(newName, name));
    _expr = std::move(newName);
}

void Ha3SqlExprGeneratorVisitor::wrapBacktick(std::string &name) {
    autil::StringUtil::replaceAll(name, BACKTICK_LITERAL, ESCAPED_BACKTICK_LITERAL);
    name = BACKTICK_LITERAL + name + BACKTICK_LITERAL;
}

std::string Ha3SqlExprGeneratorVisitor::getItemFullName(const std::string &itemName,
                                                        const std::string &itemKey) {
    std::string fullName;
    if (itemName.size() > 0 && itemName[0] == SQL_COLUMN_PREFIX) {
        fullName = itemName.substr(1);
        wrapBacktick(fullName);
    }
    if (itemKey.find("'") != std::string::npos) {
        auto newItemKey = itemKey;
        autil::StringUtil::replaceAll(newItemKey, "'", "''");
        fullName += "['" + newItemKey + "']";
    } else {
        fullName += "['" + itemKey + "']";
    }
    return fullName;
}

void Ha3SqlExprGeneratorVisitor::visitItemOp(const autil::SimpleValue &value) {
    const autil::SimpleValue &param = value[SQL_CONDITION_PARAMETER];
    string fullName
        = Ha3SqlExprGeneratorVisitor::getItemFullName(param[0].GetString(), param[1].GetString());
    string newFullName = fullName;
    ExprUtil::convertColumnName(newFullName);
    assert(newFullName != fullName);
    auto iter = _renameMapPtr->find(newFullName);
    if (iter != _renameMapPtr->end()) {
        if (iter->second != fullName) {
            setErrorInfo("rename map key[%s] conflict: [%s] vs [%s]",
                         newFullName.c_str(),
                         iter->second.c_str(),
                         fullName.c_str());
            return;
        }
    }
    _renameMapPtr->insert(make_pair(newFullName, fullName));
    _expr = newFullName;
}

} // namespace sql
