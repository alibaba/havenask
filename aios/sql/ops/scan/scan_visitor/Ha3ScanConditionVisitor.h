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
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "sql/ops/condition/ConditionVisitor.h"
#include "sql/ops/scan/Ha3ScanConditionVisitorParam.h"

namespace isearch {
namespace common {
class Query;
} // namespace common
} // namespace isearch

namespace sql {
class AndCondition;
class LeafCondition;
class NotCondition;
class OrCondition;
} // namespace sql

namespace suez {
namespace turing {
class AttributeExpression;
} // namespace turing
} // namespace suez

namespace sql {

class Ha3ScanConditionVisitor : public ConditionVisitor {
public:
    Ha3ScanConditionVisitor(const Ha3ScanConditionVisitorParam &param);
    ~Ha3ScanConditionVisitor();

public:
    void visitAndCondition(AndCondition *condition) override;
    void visitOrCondition(OrCondition *condition) override;
    void visitNotCondition(NotCondition *condition) override;
    void visitLeafCondition(LeafCondition *condition) override;

public:
    isearch::common::Query *stealQuery();
    suez::turing::AttributeExpression *stealAttributeExpression();
    const isearch::common::Query *getQuery() const;
    const suez::turing::AttributeExpression *getAttributeExpression() const;
    std::vector<suez::turing::AttributeExpression *> getQueryExprs() const;

private:
    isearch::common::Query *toTermQuery(const autil::SimpleValue &condition);
    isearch::common::Query *createTermQuery(const autil::SimpleValue &left,
                                            const autil::SimpleValue &right,
                                            const std::string &op);
    isearch::common::Query *doCreateTermQuery(const autil::SimpleValue &attr,
                                              const autil::SimpleValue &value,
                                              const std::string &op);
    bool visitUdfCondition(const autil::SimpleValue &leafCondition);

private:
    isearch::common::Query *_query;
    suez::turing::AttributeExpression *_attrExpr;
    Ha3ScanConditionVisitorParam _param;
    std::vector<suez::turing::AttributeExpression *> _queryExprs;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<Ha3ScanConditionVisitor> Ha3ScanConditionVisitorPtr;
} // namespace sql
