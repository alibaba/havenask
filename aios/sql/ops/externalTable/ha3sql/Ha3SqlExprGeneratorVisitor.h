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
#include <string>

#include "autil/legacy/RapidJsonCommon.h"
#include "sql/ops/condition/ExprGenerateVisitor.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace sql {

class Ha3SqlDynamicParamsCollector {
public:
    Ha3SqlDynamicParamsCollector(std::shared_ptr<autil::mem_pool::Pool> pool);
    ~Ha3SqlDynamicParamsCollector();
    Ha3SqlDynamicParamsCollector(const Ha3SqlDynamicParamsCollector &) = delete;
    Ha3SqlDynamicParamsCollector &operator=(const Ha3SqlDynamicParamsCollector &) = delete;

public:
    void emplace(const autil::SimpleValue &input);
    const autil::SimpleValue &getSimpleValue() const;

private:
    std::shared_ptr<autil::mem_pool::Pool> _pool;
    autil::AutilPoolAllocator _allocator;
    autil::SimpleDocument _document;
};

class Ha3SqlExprGeneratorVisitor : public ExprGenerateVisitor {
public:
    Ha3SqlExprGeneratorVisitor(ExprGenerateVisitor::VisitorMap *renameMap,
                               Ha3SqlDynamicParamsCollector *dynamicParamsCollector);
    ~Ha3SqlExprGeneratorVisitor();
    Ha3SqlExprGeneratorVisitor(const Ha3SqlExprGeneratorVisitor &) = delete;
    Ha3SqlExprGeneratorVisitor &operator=(const Ha3SqlExprGeneratorVisitor &) = delete;

protected:
    void visitInt64(const autil::SimpleValue &value) override;
    void visitDouble(const autil::SimpleValue &value) override;
    void visitBool(const autil::SimpleValue &value) override;
    void visitString(const autil::SimpleValue &value) override;
    void visitNotInOp(const autil::SimpleValue &value) override;
    void visitInOp(const autil::SimpleValue &value) override;
    void visitColumn(const autil::SimpleValue &value) override;
    void visitItemOp(const autil::SimpleValue &value) override;

private:
    void parseWithOp(const autil::SimpleValue &value, const std::string &op);

public:
    static void wrapBacktick(std::string &name);
    static std::string getItemFullName(const std::string &itemName, const std::string &itemKey);

private:
    Ha3SqlDynamicParamsCollector *_dynamicParamsCollector = nullptr;
};

typedef std::shared_ptr<Ha3SqlExprGeneratorVisitor> Ha3SqlExprGeneratorVisitorPtr;

} // namespace sql
