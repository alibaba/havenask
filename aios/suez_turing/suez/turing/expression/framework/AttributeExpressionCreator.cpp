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
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"

#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "indexlib/partition/join_cache/join_docid_reader_base.h"
#include "indexlib/partition/join_cache/join_info.h"
#include "matchdoc/MatchDocAllocator.h"
#include "suez/turing/expression/framework/AttributeExpressionFactory.h"
#include "suez/turing/expression/framework/SyntaxExpr2AttrExpr.h"
#include "suez/turing/expression/function/FunctionInterfaceCreator.h"
#include "suez/turing/expression/provider/FunctionProvider.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"
#include "turing_ops_util/variant/Tracer.h"

namespace indexlib {
namespace partition {
class PartitionReaderSnapshot;
} // namespace partition
} // namespace indexlib
namespace suez {
namespace turing {
class AttributeExpression;
class CavaPluginManager;
class FunctionProvider;
} // namespace turing
} // namespace suez

using namespace std;
using namespace indexlib::partition;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, AttributeExpressionCreator);

// op
AttributeExpressionCreator::AttributeExpressionCreator(
    autil::mem_pool::Pool *pool,                              // query
    matchdoc::MatchDocAllocator *allocator,                   // op
    const std::string &mainTableName,                         // config
    PartitionReaderSnapshot *partitionReaderSnapshot,         // query
    const TableInfoPtr &tableInfo,                            // config
    const VirtualAttributes &virtualAttributes,               // query
    const FunctionInterfaceCreator *funcCreator,              // config
    const suez::turing::CavaPluginManager *cavaPluginManager, // config
    suez::turing::FunctionProvider *functionProvider,         // op
    const std::string &subTableName,                          // query
    FunctionManagerPtr funcManager)
    : AttributeExpressionCreatorBase(pool)
    , _joinDocIdConverterCreator(pool, allocator)
    , _attrFactory(POOL_NEW_CLASS(pool,
                                  AttributeExpressionFactory,
                                  mainTableName,
                                  partitionReaderSnapshot,
                                  &_joinDocIdConverterCreator,
                                  pool,
                                  allocator,
                                  subTableName))
    , _tableInfo(tableInfo)
    , _syntaxExprValidator(tableInfo ? tableInfo->getAttributeInfos() : NULL,
                           virtualAttributes,
                           allocator ? allocator->hasSubDocAllocator() : false)
    , _funcManager(funcManager)
    , _tracer(functionProvider ? functionProvider->getRequestTracer() : nullptr) {
    if (!_funcManager) {
        _funcManager.reset(new FunctionManager(funcCreator, functionProvider, pool, cavaPluginManager));
    }
    _syntaxExprValidator.addFuncInfoMap(&funcCreator->getFuncInfoMap());
    if (cavaPluginManager) {
        _syntaxExprValidator.addCavaPluginManager(cavaPluginManager);
    }
    _convertor.initVirtualAttrs(virtualAttributes);
}

AttributeExpressionCreator::~AttributeExpressionCreator() { POOL_DELETE_CLASS(_attrFactory); }

AttributeExpression *AttributeExpressionCreator::createAtomicExpr(const string &attrName) {
    SyntaxExpr2AttrExpr visitor(_attrFactory, _funcManager.get(), _exprPool, &_convertor, _pool);
    return visitor.createAtomicExpr(attrName);
}

AttributeExpression *AttributeExpressionCreator::createAtomicExprWithoutPool(const string &attrName) {
    AttributeExpression *expr = _attrFactory->createAtomicExpr(attrName);
    _exprPool->addNeedDeleteExpr(expr);
    return expr;
}

AttributeExpression *AttributeExpressionCreator::createAttributeExpression(const SyntaxExpr *syntaxExpr,
                                                                           bool needValidate) {
    if (needValidate) {
        if (!_syntaxExprValidator.validate(syntaxExpr)) {
            TURING_LOG_AND_TRACE(WARN,
                                 "validate attribute expression [%s] fail, error msg [%s]",
                                 syntaxExpr->getExprString().c_str(),
                                 _syntaxExprValidator.getErrorMsg().c_str());
            return NULL;
        }
    }
    SyntaxExpr2AttrExpr visitor(_attrFactory, _funcManager.get(), _exprPool, &_convertor, _pool);
    syntaxExpr->accept(&visitor);
    AttributeExpression *ret = visitor.stealAttrExpr();
    if (!ret) {
        TURING_LOG_AND_TRACE(WARN, "create attribute expression [%s] fail", syntaxExpr->getExprString().c_str());
        return NULL;
    }
    return ret;
}

void AttributeExpressionCreator::resetAttrExprFactory(AttributeExpressionFactory *fakeFactory) {
    POOL_DELETE_CLASS(_attrFactory);
    _attrFactory = fakeFactory;
}

bool AttributeExpressionCreator::createJoinDocIdConverter(const std::string &joinTableName,
                                                          const std::string &mainTableName) {
    indexlib::partition::JoinInfo joinInfo;
    if (!_attrFactory->getAttributeJoinInfo(joinTableName, mainTableName, joinInfo)) {
        TURING_LOG_AND_TRACE(
            WARN, "get attribute join info failed, aux[%s], main[%s]", joinTableName.c_str(), mainTableName.c_str());
        return false;
    }
    if (_joinDocIdConverterCreator.createJoinDocIdConverter(joinInfo) == nullptr) {
        TURING_LOG_AND_TRACE(WARN,
                             "create join docid converter failed, aux[%s], main[%s]",
                             joinTableName.c_str(),
                             mainTableName.c_str());
        return false;
    }
    return true;
}

std::string AttributeExpressionCreator::getErrorMsg() const { return _syntaxExprValidator.getErrorMsg(); }

} // namespace turing
} // namespace suez
