#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/PoolVector.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <ha3/util/Log.h>
#include <suez/turing/common/SessionResource.h>
#include <suez/turing/common/QueryResource.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/common/ha3_op_common.h>

using namespace std;
using namespace tensorflow;
using namespace autil::mem_pool;

USE_HA3_NAMESPACE(search);
BEGIN_HA3_NAMESPACE(turing);

class Ha3SearcherCacheSwitchOp : public tensorflow::OpKernel
{
public:
    explicit Ha3SearcherCacheSwitchOp(tensorflow::OpKernelConstruction* ctx)
        : tensorflow::OpKernel(ctx)
    {}
public:
    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);

        SearcherQueryResource *searcherQueryResource =
            dynamic_cast<SearcherQueryResource *>(queryResource);
        OP_REQUIRES(ctx, searcherQueryResource,
                    errors::Unavailable("ha3 searcher query resource unavailable"));
        auto cacheInfo = searcherQueryResource->searcherCacheInfo;
        int32_t outPort = 0;
        if (cacheInfo) {
            if (!cacheInfo->isHit) {
                outPort = 1;
            } else {
                outPort = 2;
            }
        }
        REQUEST_TRACE_WITH_TRACER(queryResource->getTracer(), TRACE2, "switch port %d", outPort);
        ctx->set_output(outPort, ctx->input(0));
    }

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, Ha3SearcherCacheSwitchOp);

REGISTER_KERNEL_BUILDER(Name("Ha3SearcherCacheSwitchOp")
                        .Device(DEVICE_CPU),
                        Ha3SearcherCacheSwitchOp);

END_HA3_NAMESPACE(turing);
