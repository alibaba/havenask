#include <ha3/turing/ops/Ha3FillAttributesOp.h>

using namespace std;
using namespace matchdoc;
using namespace tensorflow;
using namespace suez::turing;

USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(turing);
USE_HA3_NAMESPACE(func_expression);

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
        HA3_LOG(WARN, "create pkExpr [%s] failed.", exprName.c_str());
        return false;
    }
    pkExpr->setOriginalString(refName);
    if (!pkExpr->allocate(matchDocAllocator.get())) {
        HA3_LOG(WARN, "allocate refer [%s] failed.", refName.c_str());
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
            HA3_LOG(WARN, "get pk info failed, no primary key index found.");
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
            HA3_LOG(WARN, "Create attribute expression failed! "
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
    evaluator.batchEvaluateExpressions(&matchDocVec[0], matchDocVec.size());

    return true;
}

HA3_LOG_SETUP(turing, Ha3FillAttributesOp);

REGISTER_KERNEL_BUILDER(Name("Ha3FillAttributesOp")
                        .Device(DEVICE_CPU),
                        Ha3FillAttributesOp);

END_HA3_NAMESPACE(turing);
