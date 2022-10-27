#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <ha3/common/ha3_op_common.h>
#include <suez/turing/common/SessionResource.h>
#include <ha3/util/Log.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/turing/ops/SearcherSessionResource.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

USE_HA3_NAMESPACE(monitor);
BEGIN_HA3_NAMESPACE(turing);

class Ha3EndIdentityOp : public tensorflow::OpKernel
{
public:
    explicit Ha3EndIdentityOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        SearcherQueryResource *searcherQueryResource =
            dynamic_cast<SearcherQueryResource *>(queryResource);
        OP_REQUIRES(ctx, searcherQueryResource,
                    errors::Unavailable("searcher query resource unavailable."));
        auto metricsCollector = searcherQueryResource->sessionMetricsCollector;;
        OP_REQUIRES(ctx, metricsCollector,
                    errors::Unavailable("metrics collector unavailable."));
        metricsCollector->graphRunEndTrigger();

        if (IsRefType(ctx->input_dtype(0))) {
            ctx->forward_ref_input_to_ref_output(0, 0);
        } else {
            ctx->set_output(0, ctx->input(0));
        }
    }

private:
    HA3_LOG_DECLARE();

};

HA3_LOG_SETUP(turing, Ha3EndIdentityOp);

REGISTER_KERNEL_BUILDER(Name("Ha3EndIdentityOp")
                        .Device(DEVICE_CPU),
                        Ha3EndIdentityOp);

END_HA3_NAMESPACE(turing);
