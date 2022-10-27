#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <iostream>
#include <ha3/common/ha3_op_common.h>
#include <ha3/util/Log.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/common/Tracer.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/turing/ops/SearcherSessionResource.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(turing);

class Ha3ResultReconstructOp : public tensorflow::OpKernel
{
public:
    explicit Ha3ResultReconstructOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {     
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto resultTensor = ctx->input(0).scalar<Variant>()();
        auto ha3ResultVariant = resultTensor.get<Ha3ResultVariant>();
        ResultPtr result = ha3ResultVariant->getResult();
        OP_REQUIRES(ctx, result, errors::Unavailable("ha3 result unavailable"));
        
        auto matchdocsTensor = ctx->input(1).scalar<Variant>()();
        auto matchDocsVariant = matchdocsTensor.get<MatchDocsVariant>();
        OP_REQUIRES(ctx, matchDocsVariant, 
                    errors::Unavailable("xxxxx ha3 searcher resource unavailable"));

        uint32_t totalMatchDocs = ctx->input(2).scalar<uint32_t>()();
        uint32_t actualMatchDocs = ctx->input(3).scalar<uint32_t>()();        

        matchdoc::MatchDocAllocatorPtr allocator = matchDocsVariant->getAllocator();
        Ha3MatchDocAllocatorPtr ha3Allocator = 
            dynamic_pointer_cast<Ha3MatchDocAllocator>(allocator);
        OP_REQUIRES(ctx, ha3Allocator, errors::Unavailable("dynamic_pointer_cast allocator failed."));

        MatchDocs *matchDocs = result->getMatchDocs();
        vector<matchdoc::MatchDoc> matchDocVec = matchDocsVariant->getMatchDocs();
        if (matchDocs) {
            matchDocs->getMatchDocsVect() = matchDocVec;
            matchDocs->setMatchDocAllocator(ha3Allocator);
            matchDocs->setTotalMatchDocs(totalMatchDocs);
            matchDocs->setActualMatchDocs(actualMatchDocs);
        }

        result->setSortExprMeta(matchDocsVariant->getSortMetas());
        Tensor* resultOutTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &resultOutTensor));
        resultOutTensor->scalar<Variant>()() = *ha3ResultVariant;
        
    }

private:
    HA3_LOG_DECLARE();

};

HA3_LOG_SETUP(turing, Ha3ResultReconstructOp);

REGISTER_KERNEL_BUILDER(Name("Ha3ResultReconstructOp")
                        .Device(DEVICE_CPU),
                        Ha3ResultReconstructOp);

END_HA3_NAMESPACE(turing);
