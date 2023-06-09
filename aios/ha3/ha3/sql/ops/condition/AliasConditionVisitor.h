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
#include <unordered_map>

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

class AliasConditionVisitor : public ConditionVisitor {
public:
    AliasConditionVisitor();
    ~AliasConditionVisitor();

public:
    void visitAndCondition(AndCondition *condition) override;
    void visitOrCondition(OrCondition *condition) override;
    void visitNotCondition(NotCondition *condition) override;
    void visitLeafCondition(LeafCondition *condition) override;

public:
    std::unordered_map<std::string, std::string> &getAliasMap() {
        return _aliasMap;
    }

private:
    std::unordered_map<std::string, std::string> _aliasMap;
};

typedef std::shared_ptr<AliasConditionVisitor> AliasConditionVisitorPtr;
} // namespace sql
} // namespace isearch
