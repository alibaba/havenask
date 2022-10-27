#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <iostream>
#include <ha3/common/ha3_op_common.h>
#include <suez/turing/common/SessionResource.h>
#include <ha3/util/Log.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/turing/ops/SearcherSessionResource.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/service/SearcherResource.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(turing);
USE_HA3_NAMESPACE(func_expression);

class Ha3ReleaseRedundantOp : public tensorflow::OpKernel
{
public:
    explicit Ha3ReleaseRedundantOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {     
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        OP_REQUIRES(ctx, sessionResource, 
                    errors::Unavailable("session resource unavailable"));

        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        SearcherQueryResource *searcherQueryResource = 
            dynamic_cast<SearcherQueryResource *>(queryResource);
        OP_REQUIRES(ctx, searcherQueryResource, 
                    errors::Unavailable("ha3 searcher query resource unavailable"));

        SearchRuntimeResourcePtr runtimeResource = searcherQueryResource->runtimeResource;
        OP_REQUIRES(ctx, runtimeResource, 
                    errors::Unavailable("ha3 runtime resource unavailable"));

        auto requiredTopK = runtimeResource->docCountLimits.requiredTopK;
        auto resultTensor = ctx->input(0).scalar<Variant>()();
        auto ha3ResultVariant = resultTensor.get<Ha3ResultVariant>();
        ResultPtr result = ha3ResultVariant->getResult();
        OP_REQUIRES(ctx, result, errors::Unavailable("ha3 result unavailable"));

        
        MatchDocs *matchDocs = result->getMatchDocs();
        if (matchDocs) {
            auto ha3MatchDocAllocator = matchDocs->getMatchDocAllocator();
            if (ha3MatchDocAllocator) {
                std::vector<matchdoc::MatchDoc> &matchDocVect = matchDocs->getMatchDocsVect();
                releaseRedundantMatchDocs(matchDocVect, requiredTopK, ha3MatchDocAllocator);
            }
        }
        ctx->set_output(0, ctx->input(0));
    }

private:
    void releaseRedundantMatchDocs(std::vector<matchdoc::MatchDoc> &matchDocVect,
                                   uint32_t requiredTopK,
                                   const common::Ha3MatchDocAllocatorPtr &matchDocAllocator) {
        uint32_t size = (uint32_t)matchDocVect.size();
        while(size > requiredTopK)
        {
            auto matchDoc = matchDocVect[--size];
            matchDocAllocator->deallocate(matchDoc);
        }
        matchDocVect.erase(matchDocVect.begin() + size, matchDocVect.end());        
    }

private:
    HA3_LOG_DECLARE();

};

HA3_LOG_SETUP(turing, Ha3ReleaseRedundantOp);

REGISTER_KERNEL_BUILDER(Name("Ha3ReleaseRedundantOp")
                        .Device(DEVICE_CPU),
                        Ha3ReleaseRedundantOp);

END_HA3_NAMESPACE(turing);
