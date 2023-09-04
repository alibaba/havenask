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
#include <string>

#include "autil/Log.h"
#include "suez/turing/expression/framework/SyntaxExpr2AttrExpr.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace matchdoc {
class MatchDocAllocator;
class ReferenceBase;
} // namespace matchdoc

namespace suez {
namespace turing {
class AttributeExpression;
class AttributeExpressionPool;
class FunctionManager;
class RankSyntaxExpr;

class MatchDocsSyntaxExpr2AttrExpr : public SyntaxExpr2AttrExpr {
public:
    MatchDocsSyntaxExpr2AttrExpr(matchdoc::MatchDocAllocator *allocator,
                                 FunctionManager *funcManager,
                                 AttributeExpressionPool *exprPool,
                                 autil::mem_pool::Pool *pool,
                                 const std::map<std::string, std::string> *fieldMapping);

    virtual ~MatchDocsSyntaxExpr2AttrExpr();

public:
    void visitRankSyntaxExpr(const RankSyntaxExpr *syntaxExpr) override;
    AttributeExpression *createAtomicExpr(const std::string &attrName,
                                          const std::string &prefixName,
                                          const std::string &completeAttrName) override;

protected:
    AttributeExpression *createAttributeExpression(matchdoc::ReferenceBase *refer, autil::mem_pool::Pool *pool);
    template <typename T>
    AttributeExpression *doCreateAttributeExpression(matchdoc::ReferenceBase *refer, autil::mem_pool::Pool *pool);

protected:
    matchdoc::MatchDocAllocator *_allocator;
    const std::map<std::string, std::string> *_fieldMapping;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
