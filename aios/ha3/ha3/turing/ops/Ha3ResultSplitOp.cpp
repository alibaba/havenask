#include <autil/Log.h>
#include <autil/mem_pool/Pool.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <ha3/common/ha3_op_common.h>
#include <ha3/util/Log.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/turing/ops/SearcherSessionResource.h>
#include <ha3/qrs/MatchDocs2Hits.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace autil::mem_pool;

USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(qrs);
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(turing);

class Ha3ResultSplitOp : public tensorflow::OpKernel
{
public:
    explicit Ha3ResultSplitOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {     
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));
 
        auto resultTensor = ctx->input(1).scalar<Variant>()();
        auto ha3ResultVariant = resultTensor.get<Ha3ResultVariant>();
        ResultPtr result = ha3ResultVariant->getResult();
        OP_REQUIRES(ctx, result, errors::Unavailable("ha3 result unavailable"));

        MatchDocs *matchDocs = result->getMatchDocs();
        OP_REQUIRES(ctx, matchDocs, errors::Unavailable("matchDocs unavailable"));
        auto ha3AllocatorPtr = matchDocs->getMatchDocAllocator();
        
        matchdoc::MatchDocAllocatorPtr allocatorPtr =
            dynamic_pointer_cast<matchdoc::MatchDocAllocator>(ha3AllocatorPtr);
        Pool *pool = request->getPool();
        MatchDocsVariant matchDocsVariant(allocatorPtr, pool);
        
        vector<matchdoc::MatchDoc> matchDocVect;
        matchDocs->stealMatchDocs(matchDocVect);        
        matchDocsVariant.stealMatchDocs(matchDocVect);
        
        const auto *configClause = request->getConfigClause();
        OP_REQUIRES(ctx, configClause, errors::Unavailable("configClause unavailable"));
        auto requiredTopK = configClause->getStartOffset() + configClause->getHitCount();
        
        uint32_t totalMatchDocs = matchDocs->totalMatchDocs();
        uint32_t actualMatchDocs = matchDocs->actualMatchDocs();
        uint16_t srcCount = result->getSrcCount();
        
        Tensor* matchDocsOut = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &matchDocsOut));
        matchDocsOut->scalar<Variant>()() = matchDocsVariant;

        Tensor* extraDocs = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(1, {}, &extraDocs));
        extraDocs->scalar<uint32_t>()() = (uint32_t)requiredTopK;
        
        Tensor* totalDocs = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(2, {}, &totalDocs));
        totalDocs->scalar<uint32_t>()() = totalMatchDocs;        

        Tensor* actualDocs = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(3, {}, &actualDocs));
        actualDocs->scalar<uint32_t>()() = actualMatchDocs;

        Tensor* resultOutTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(4, {}, &resultOutTensor));
        resultOutTensor->scalar<Variant>()() = *ha3ResultVariant;

        Tensor* srcCountTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(5, {}, &srcCountTensor));
        srcCountTensor->scalar<uint16_t>()() = srcCount;        
    }

private:
    HA3_LOG_DECLARE();

};

HA3_LOG_SETUP(turing, Ha3ResultSplitOp);

REGISTER_KERNEL_BUILDER(Name("Ha3ResultSplitOp")
                        .Device(DEVICE_CPU),
                        Ha3ResultSplitOp);

END_HA3_NAMESPACE(turing);
