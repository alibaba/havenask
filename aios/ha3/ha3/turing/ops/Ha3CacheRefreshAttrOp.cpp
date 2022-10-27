#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/PoolVector.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <ha3/util/Log.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/turing/ops/SearcherSessionResource.h>
#include <ha3/common/ha3_op_common.h>
#include <suez/turing/expression/framework/ExpressionEvaluator.h>
#include <suez/turing/expression/framework/AttributeExpressionFactory.h>
#include <ha3/search/SearcherCacheManager.h>
#include <ha3/search/SearcherCacheInfo.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace autil::mem_pool;

USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(turing);

class Ha3CacheRefreshAttrOp : public tensorflow::OpKernel
{
public:
    explicit Ha3CacheRefreshAttrOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);

        SearcherQueryResource *searcherQueryResource = dynamic_cast<SearcherQueryResource *>(queryResource);
        OP_REQUIRES(ctx, searcherQueryResource, errors::Unavailable("ha3 searcher query resource unavailable"));

        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));

        auto resultTensor = ctx->input(1).scalar<Variant>()();
        auto ha3ResultVariant = resultTensor.get<Ha3ResultVariant>();
        ResultPtr result = ha3ResultVariant->getResult();
        OP_REQUIRES(ctx, result, errors::Unavailable("ha3 result unavailable"));

        SearcherCacheInfoPtr cacheInfo = searcherQueryResource->searcherCacheInfo;
        OP_REQUIRES(ctx, cacheInfo, errors::Unavailable("cache info unavailable"));

        SearcherCacheManagerPtr cacheManager = cacheInfo->cacheManager;
        OP_REQUIRES(ctx, cacheManager, errors::Unavailable("cache manager unavailable"));

        CacheResult *cacheResult = cacheManager->getCacheResult();
        refreshAttributes(cacheResult->getResult(), request.get(),
                          searcherQueryResource, request->getPool());

        Tensor  *resultsTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {2}, &resultsTensor));
        auto flat = resultsTensor->flat<Variant>();
        flat(0) = Ha3ResultVariant(ResultPtr(cacheResult->stealResult()), request->getPool());
        flat(1) = *ha3ResultVariant;
    }

private:
    void refreshAttributes(Result *result, Request *request,
                           SearcherQueryResource *searcherQueryResource,
                           autil::mem_pool::Pool *pool)
    {
        auto partitionResource = searcherQueryResource->partitionResource;
        auto commonResource = searcherQueryResource->commonResource;
        MatchDocs *matchDocs = result->getMatchDocs();
        if (!matchDocs || matchDocs->size() == 0) {
            return;
        }
        ExpressionVector needRefreshAttrs;

        const auto &allocator = matchDocs->getMatchDocAllocator();
        JoinDocIdConverterCreator *docIdConverterFactory = POOL_NEW_CLASS(
                pool, JoinDocIdConverterCreator, pool, allocator.get());
        AttributeExpressionFactory *attrExprFactory = POOL_NEW_CLASS(
                pool, AttributeExpressionFactory,
                partitionResource->mainTable,
                partitionResource->partitionReaderSnapshot.get(),
                docIdConverterFactory,
                pool, allocator.get());

        initRefreshAttrs(allocator, docIdConverterFactory, attrExprFactory,
                         request->getSearcherCacheClause(), needRefreshAttrs);
        if (needRefreshAttrs.size() == 0) {
            POOL_DELETE_CLASS(attrExprFactory);
            POOL_DELETE_CLASS(docIdConverterFactory);
            return;
        }
        ExpressionEvaluator<ExpressionVector> evaluator(
                needRefreshAttrs, allocator->getSubDocAccessor());
        auto &matchDocVec = matchDocs->getMatchDocsVect();
        evaluator.batchEvaluateExpressions(&matchDocVec[0], matchDocVec.size());
        for (size_t i = 0; i < needRefreshAttrs.size(); i++) {
            POOL_DELETE_CLASS(needRefreshAttrs[i]);
        }

        POOL_DELETE_CLASS(attrExprFactory);
        POOL_DELETE_CLASS(docIdConverterFactory);
    }

    void initRefreshAttrs(
            const common::Ha3MatchDocAllocatorPtr &vsa,
            JoinDocIdConverterCreator *docIdConverterFactory,
            AttributeExpressionFactory *attrExprFactory,
            SearcherCacheClause *searcherCacheClause,
            ExpressionVector &needRefreshAttrs)
    {
        assert(searcherCacheClause != NULL);
        const set<string> &refreshAttrNames =
            searcherCacheClause->getRefreshAttributes();
        vector<string> needRefreshAttrNames;
        for (set<string>::const_iterator it = refreshAttrNames.begin();
             it != refreshAttrNames.end(); it++)
        {
            if (vsa->findReferenceWithoutType(*it) != NULL) {
                needRefreshAttrNames.push_back(*it);
            }
        }
        if (needRefreshAttrNames.size() == 0) {
            return;
        }
        createNeedRefreshAttrExprs(vsa, docIdConverterFactory, attrExprFactory,
                needRefreshAttrNames, needRefreshAttrs);
    }

    void createNeedRefreshAttrExprs(
            const common::Ha3MatchDocAllocatorPtr &allocator,
            JoinDocIdConverterCreator *docIdConverterFactory,
            AttributeExpressionFactory *attrExprFactory,
            const vector<string> &needRefreshAttrNames,
            ExpressionVector &needRefreshAttrs)
    {
        for (size_t i = 0; i < needRefreshAttrNames.size(); i++) {
            const string &attrName = needRefreshAttrNames[i];
            auto refBase = allocator->findReferenceWithoutType(attrName);
            AttributeExpression *expr = attrExprFactory->createAtomicExpr(attrName);
            if (!expr) {
                HA3_LOG(WARN, "failed to createAtomicExpr for %s", attrName.c_str());
                continue;
            }
            auto valueType = refBase->getValueType();
            auto vt = valueType.getBuiltinType();
            bool isMulti = valueType.isMultiValue();
#define ATTR_TYPE_CASE_HELPER(vt_type)                                  \
            case vt_type:                                               \
                if (isMulti) {                                          \
                    typedef VariableTypeTraits<vt_type, true>::AttrExprType AttrExprType; \
                    AttributeExpressionTyped<AttrExprType> *typedExpr   \
                        = dynamic_cast<AttributeExpressionTyped<AttrExprType> *>(expr); \
                    assert(typedExpr != NULL);                          \
                    auto ref = dynamic_cast<matchdoc::Reference<AttrExprType> *>(refBase); \
                    typedExpr->setReference(ref);                       \
                } else {                                                \
                    typedef VariableTypeTraits<vt_type, false>::AttrExprType AttrExprType; \
                    AttributeExpressionTyped<AttrExprType> *typedExpr   \
                        = dynamic_cast<AttributeExpressionTyped<AttrExprType> *>(expr); \
                    assert(typedExpr != NULL);                          \
                    auto ref = dynamic_cast<matchdoc::Reference<AttrExprType> *>(refBase); \
                    typedExpr->setReference(ref);                       \
                }                                                       \
                needRefreshAttrs.push_back(expr);                       \
                break

            switch (vt) {
            NUMERIC_VARIABLE_TYPE_MACRO_HELPER(ATTR_TYPE_CASE_HELPER);
            ATTR_TYPE_CASE_HELPER(vt_string);
            ATTR_TYPE_CASE_HELPER(vt_hash_128);
            default:
                break;
            }
#undef ATTR_TYPE_CASE_HELPER
        }
    }
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, Ha3CacheRefreshAttrOp);

REGISTER_KERNEL_BUILDER(Name("Ha3CacheRefreshAttrOp")
                        .Device(DEVICE_CPU),
                        Ha3CacheRefreshAttrOp);

END_HA3_NAMESPACE(turing);
