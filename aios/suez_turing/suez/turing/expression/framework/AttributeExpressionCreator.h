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

#include <string>

#include "autil/Log.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/framework/AttributeExpressionCreatorBase.h"
#include "suez/turing/expression/framework/AttributeExpressionPool.h"
#include "suez/turing/expression/framework/JoinDocIdConverterCreator.h"
#include "suez/turing/expression/function/FunctionManager.h"
#include "suez/turing/expression/syntax/SyntaxExprValidator.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "suez/turing/expression/util/VirtualAttrConvertor.h"
#include "suez/turing/expression/util/VirtualAttribute.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace indexlib {
namespace partition {
class PartitionReaderSnapshot;
} // namespace partition
} // namespace indexlib
namespace matchdoc {
class MatchDocAllocator;
} // namespace matchdoc

namespace suez {
namespace turing {

class AttributeExpressionFactory;
class AttributeExpression;
class FunctionInterfaceCreator;
class FunctionProvider;
class SyntaxExpr;
class Tracer;

class AttributeExpressionCreator : public AttributeExpressionCreatorBase {
public:
    AttributeExpressionCreator(autil::mem_pool::Pool *pool,
                               matchdoc::MatchDocAllocator *allocator,
                               const std::string &mainTableName,
                               indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot,
                               const TableInfoPtr &tableInfo,
                               const VirtualAttributes &virtualAttributes,
                               const FunctionInterfaceCreator *funcCreator,
                               const suez::turing::CavaPluginManager *cavaPluginManager,
                               FunctionProvider *functionProvider,
                               const std::string &subTableName = "",
                               FunctionManagerPtr funcManager = {});

    ~AttributeExpressionCreator();

private:
    AttributeExpressionCreator(const AttributeExpressionCreator &);
    AttributeExpressionCreator &operator=(const AttributeExpressionCreator &);

public:
    using AttributeExpressionCreatorBase::createAttributeExpression;
    // virtual for fake
    virtual AttributeExpression *createAtomicExpr(const std::string &attrName);

    virtual AttributeExpression *createAttributeExpression(const SyntaxExpr *syntaxExpr, bool needValidate = false);

    virtual AttributeExpression *createAtomicExprWithoutPool(const std::string &attrName);

    void addPair(const std::string &exprStr, AttributeExpression *attrExpr, bool needDelete = true) {
        _exprPool->addPair(exprStr, attrExpr, needDelete);
    }
    JoinDocIdConverterCreator *getJoinDocIdConverterCreator() { return &_joinDocIdConverterCreator; }
    FunctionManager *getFunctionManager() { return _funcManager.get(); }
    bool createJoinDocIdConverter(const std::string &joinTableName, const std::string &mainTableName);
    std::string getErrorMsg() const;

private:
    // for test
    virtual void resetAttrExprFactory(AttributeExpressionFactory *fakeFactory);

protected:
    JoinDocIdConverterCreator _joinDocIdConverterCreator;
    AttributeExpressionFactory *_attrFactory;
    TableInfoPtr _tableInfo; // compatible with ha3 qrs
    SyntaxExprValidator _syntaxExprValidator;
    FunctionManagerPtr _funcManager;
    VirtualAttrConvertor _convertor;
    suez::turing::Tracer *_tracer = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(AttributeExpressionCreator);

} // namespace turing
} // namespace suez
