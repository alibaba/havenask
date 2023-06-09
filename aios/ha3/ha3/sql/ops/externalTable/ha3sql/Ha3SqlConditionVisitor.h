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
#include <sstream>
#include <string>

#include "autil/Log.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "ha3/sql/ops/condition/ConditionVisitor.h"
#include "ha3/sql/ops/externalTable/ha3sql/Ha3SqlExprGeneratorVisitor.h"

namespace autil {

namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace isearch {
namespace sql {
class AndCondition;
class NotCondition;
class OrCondition;
} // namespace sql
} // namespace isearch

namespace isearch {
namespace sql {

class Ha3SqlConditionVisitor : public ConditionVisitor {
public:
    Ha3SqlConditionVisitor(std::shared_ptr<autil::mem_pool::Pool> pool);
    ~Ha3SqlConditionVisitor();

public:
    void visitAndCondition(AndCondition *condition) override;
    void visitOrCondition(OrCondition *condition) override;
    void visitNotCondition(NotCondition *condition) override;
    void visitLeafCondition(LeafCondition *condition) override;

public:
    std::string getConditionStr() const;
    std::string getDynamicParamsStr() const;

private:
    std::stringstream _ss;
    std::unordered_map<std::string, std::string> _renameMap;
    Ha3SqlDynamicParamsCollector _dynamicParamsCollector;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace sql
} // namespace isearch
