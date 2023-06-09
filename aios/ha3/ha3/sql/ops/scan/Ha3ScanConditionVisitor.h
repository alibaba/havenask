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

#include <map>
#include <memory>
#include <stddef.h>
#include <string>

#include "autil/Log.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/sql/ops/condition/ConditionVisitor.h"
#include "ha3/turing/common/ModelConfig.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "ha3/sql/ops/scan/Ha3ScanConditionVisitorParam.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace build_service {
namespace analyzer {
class AnalyzerFactory;
} // namespace analyzer
} // namespace build_service
namespace isearch {
namespace common {
class Query;
class QueryInfo;
} // namespace common
namespace search {
class LayerMeta;
class QueryExecutor;
} // namespace search
namespace sql {
class AndCondition;
class IndexInfo;
class LeafCondition;
class NotCondition;
class OrCondition;
class UdfToQueryManager;
class Ha3ScanConditionVisitorParam;
} // namespace sql
} // namespace isearch
namespace suez {
namespace turing {
class AttributeExpression;
class IndexInfos;
class TimeoutTerminator;
} // namespace turing
} // namespace suez

namespace isearch {
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
    common::Query *stealQuery();
    suez::turing::AttributeExpression *stealAttributeExpression();
    const common::Query *getQuery() const;
    const suez::turing::AttributeExpression *getAttributeExpression() const;
    std::vector<suez::turing::AttributeExpression *> getQueryExprs() const;
private:
    common::Query *toTermQuery(const autil::SimpleValue &condition);
    common::Query *createTermQuery(const autil::SimpleValue &left,
                                   const autil::SimpleValue &right,
                                   const std::string &op);
    common::Query *doCreateTermQuery(const autil::SimpleValue &attr,
                                     const autil::SimpleValue &value,
                                     const std::string &op);
    bool visitUdfCondition(const autil::SimpleValue &leafCondition);

private:
    common::Query *_query;
    suez::turing::AttributeExpression *_attrExpr;
    Ha3ScanConditionVisitorParam _param;
    std::vector<suez::turing::AttributeExpression *> _queryExprs;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<Ha3ScanConditionVisitor> Ha3ScanConditionVisitorPtr;
} // namespace sql
} // namespace isearch
