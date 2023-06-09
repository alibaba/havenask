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
#include "ha3/turing/ops/Ha3FillAttributesOp.h"

#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "turing_ops_util/util/OpUtil.h"
#include "turing_ops_util/variant/MatchDocsVariant.h"
#include "indexlib/index_define.h"
#include "indexlib/misc/common.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/ValueType.h"
#include "matchdoc/VectorDocStorage.h"
#include "suez/turing/common/PoolConfig.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/framework/ExpressionEvaluator.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"

#include "ha3/common/AttributeClause.h"
#include "ha3/common/AttributeItem.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/GlobalIdentifier.h"
#include "ha3/common/MultiErrorResult.h"
#include "ha3/common/Request.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/isearch.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/service/ServiceDegrade.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "autil/Log.h"

using namespace std;
using namespace matchdoc;
using namespace tensorflow;
using namespace suez::turing;

using namespace isearch::rank;
using namespace isearch::service;
using namespace isearch::common;
using namespace indexlib::index;
using namespace isearch::search;
using namespace isearch::config;
namespace isearch {
namespace turing {

void Ha3FillAttributesOp::Compute(tensorflow::OpKernelContext* ctx) {
    auto sessionResource = GET_SESSION_RESOURCE();
    auto queryResource = GET_QUERY_RESOURCE(sessionResource);
    SearcherQueryResource *searcherQueryResource =
        dynamic_cast<SearcherQueryResource *>(queryResource);
    OP_REQUIRES(ctx, searcherQueryResource,
                errors::Unavailable("ha3 searcher query resource unavailable"));

    auto requestTensor = ctx->input(0).scalar<Variant>()();
    auto requestVariant = requestTensor.get<Ha3RequestVariant>();
    RequestPtr request = requestVariant->getRequest();
    auto matchdocsTensor = ctx->input(1).scalar<Variant>()();
    auto matchDocsVariant = matchdocsTensor.get<MatchDocsVariant>();

    common::Ha3MatchDocAllocatorPtr ha3MatchDocAllocator =
        dynamic_pointer_cast<common::Ha3MatchDocAllocator>(matchDocsVariant->getAllocator());
    OP_REQUIRES(ctx, ha3MatchDocAllocator,
                errors::Unavailable("dynamic_pointer_cast matchDocAllocator failed."));

    SearchPartitionResourcePtr partitionResource = searcherQueryResource->partitionResource;
    OP_REQUIRES(ctx, partitionResource,
                errors::Unavailable("get partition resource failed."));

    SearchCommonResourcePtr commonResource = searcherQueryResource->commonResource;
    OP_REQUIRES(ctx, commonResource,
                errors::Unavailable("get common resource failed."));

    vector<matchdoc::MatchDoc> matchDocVec;
    matchDocsVariant->stealMatchDocs(matchDocVec);

    bool suc = false;
    suc = fillPk(request->getConfigClause(), matchDocVec,
                 ha3MatchDocAllocator,
                 *partitionResource->attributeExpressionCreator,
                 commonResource);

    OP_REQUIRES(ctx, suc,
                errors::Unavailable("fillPk failed."));

    fillAttribute(request->getAttributeClause(), matchDocVec,
                  ha3MatchDocAllocator,
                  *partitionResource->attributeExpressionCreator);

    matchDocsVariant->stealMatchDocs(matchDocVec);
    Tensor* matchDocs = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &matchDocs));
    matchDocs->scalar<Variant>()() = *matchDocsVariant;
}

bool Ha3FillAttributesOp::fillPk(const common::ConfigClause *configClause,
            std::vector<matchdoc::MatchDoc> &matchDocVec,
            Ha3MatchDocAllocatorPtr &matchDocAllocator,
            AttributeExpressionCreator &attributeExpressionCreator,
            SearchCommonResourcePtr &commonResource)
{
    uint8_t phaseOneInfoMask = configClause->getPhaseOneBasicInfoMask();
    matchDocAllocator->initPhaseOneInfoReferences(phaseOneInfoMask);

    std::string exprName;
    std::string refName;
    if (!getPKExprInfo(phaseOneInfoMask, exprName, refName, commonResource)) {
        return false;
    }
    bool isPKNecessary = configClause->getFetchSummaryType() != BY_DOCID;
    AttributeExpression *pkExpr = attributeExpressionCreator.createAtomicExprWithoutPool(exprName);
    if (!isPKNecessary && !pkExpr) {
        return true;
    }
    if (!pkExpr) {
        commonResource->errorResult->addError(ERROR_CREATE_PK_EXPR, exprName);
        AUTIL_LOG(WARN, "create pkExpr [%s] failed.", exprName.c_str());
        return false;
    }
    pkExpr->setOriginalString(refName);
    if (!pkExpr->allocate(matchDocAllocator.get())) {
        AUTIL_LOG(WARN, "allocate refer [%s] failed.", refName.c_str());
        return false;
    }
    matchDocAllocator->extend();
    auto refer = pkExpr->getReferenceBase();
    refer->setSerializeLevel(SL_CACHE);
    std::vector<AttributeExpression *> attributeExpressionVec;
    attributeExpressionVec.push_back(pkExpr);
    ExpressionEvaluator<std::vector<AttributeExpression *> > evaluator(
            attributeExpressionVec,
            matchDocAllocator->getSubDocAccessor());
    evaluator.batchEvaluateExpressions(&matchDocVec[0], matchDocVec.size());
    return true;
}

bool Ha3FillAttributesOp::getPKExprInfo(uint8_t phaseOneInfoMask,
                   std::string &exprName, std::string &refName,
                   SearchCommonResourcePtr &commonResource)
{
    if (PHASE_ONE_HAS_RAWPK(phaseOneInfoMask)) {
        const IndexInfo *indexInfo = commonResource->tableInfo->getPrimaryKeyIndexInfo();
        if (!indexInfo) {
            AUTIL_LOG(WARN, "get pk info failed, no primary key index found.");
            return false;
        }
        exprName = indexInfo->fieldName;
        refName = RAW_PRIMARYKEY_REF;
        return true;
    }
    exprName = PRIMARYKEY_REF;
    refName = PRIMARYKEY_REF;
    return true;
}

bool Ha3FillAttributesOp::fillAttribute(const common::AttributeClause *attributeClause,
                   std::vector<matchdoc::MatchDoc> &matchDocVec,
                   Ha3MatchDocAllocatorPtr &matchDocAllocator,
                   AttributeExpressionCreator &attributeExpressionCreator) {
    if (!attributeClause) {
        return true;
    }
    std::vector<AttributeExpression *> attributeExpressionVec;
    const std::set<std::string> &attributeNames = attributeClause->getAttributeNames();
    attributeExpressionVec.reserve(attributeNames.size());
    MatchDocAllocator::ReferenceAliasMapPtr aliasMap =
        matchDocAllocator->getReferenceAliasMap();
    if (!aliasMap) {
        aliasMap.reset(new MatchDocAllocator::ReferenceAliasMap);
    }
    for (std::set<std::string>::const_iterator it = attributeNames.begin();
         it != attributeNames.end(); ++it)
    {
        const std::string &attributeName = *it;

        AttributeExpression *expr
            = attributeExpressionCreator.createAtomicExpr(attributeName);

        if (!expr || !expr->allocate(matchDocAllocator.get())) {
            AUTIL_LOG(WARN, "Create attribute expression failed! "
                    "attributeName[%s]", attributeName.c_str());
            return false;
        }
        aliasMap->insert(make_pair(attributeName, expr->getOriginalString()));
        auto refer = expr->getReferenceBase();
        refer->setSerializeLevel(SL_ATTRIBUTE);
        matchdoc::ValueType valueType = refer->getValueType();
        valueType._ha3ReservedFlag = 1;
        refer->setValueType(valueType);
        attributeExpressionVec.push_back(expr);
    }
    matchDocAllocator->setReferenceAliasMap(aliasMap);
    matchDocAllocator->extend();
    ExpressionEvaluator<std::vector<AttributeExpression *> > evaluator(
            attributeExpressionVec,
            matchDocAllocator->getSubDocAccessor());
    evaluator.batchEvaluated(&matchDocVec[0], matchDocVec.size());

    return true;
}

AUTIL_LOG_SETUP(ha3, Ha3FillAttributesOp);

REGISTER_KERNEL_BUILDER(Name("Ha3FillAttributesOp")
                        .Device(DEVICE_CPU),
                        Ha3FillAttributesOp);

} // namespace turing
} // namespace isearch
