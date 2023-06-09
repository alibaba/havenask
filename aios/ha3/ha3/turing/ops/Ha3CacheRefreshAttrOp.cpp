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
#include <assert.h>
#include <cstddef>
#include <memory>
#include <set>
#include <string>
#include <memory>
#include <vector>

#include "alog/Logger.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "turing_ops_util/util/OpUtil.h"
#include "matchdoc/Reference.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpressionFactory.h"
#include "suez/turing/expression/framework/ExpressionEvaluator.h"
#include "suez/turing/expression/framework/JoinDocIdConverterCreator.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/types.pb.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"

#include "ha3/common/AttributeItem.h"
#include "ha3/common/ClauseBase.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/common/SearcherCacheClause.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/search/CacheResult.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/SearcherCacheInfo.h"
#include "ha3/search/SearcherCacheManager.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "autil/Log.h" // IWYU pragma: keep

using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace autil::mem_pool;

using namespace isearch::rank;
using namespace isearch::common;
using namespace isearch::search;
using namespace isearch::config;
namespace isearch {
namespace turing {

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
                partitionResource->partitionReaderSnapshot,
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
                AUTIL_LOG(WARN, "failed to createAtomicExpr for %s", attrName.c_str());
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
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(ha3, Ha3CacheRefreshAttrOp);

REGISTER_KERNEL_BUILDER(Name("Ha3CacheRefreshAttrOp")
                        .Device(DEVICE_CPU),
                        Ha3CacheRefreshAttrOp);

} // namespace turing
} // namespace isearch
