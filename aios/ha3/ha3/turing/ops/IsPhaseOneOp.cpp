#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <iostream>
#include <ha3/common/ha3_op_common.h>
#include <suez/turing/common/SessionResource.h>
#include <ha3/util/Log.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <ha3/turing/ops/SearcherQueryResource.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(monitor);
BEGIN_HA3_NAMESPACE(turing);

class IsPhaseOneOp : public tensorflow::OpKernel
{
public:
    explicit IsPhaseOneOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
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
        metricsCollector->graphRunStartTrigger();

        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));
        uint32_t phaseNumber = request->getConfigClause()->getPhaseNumber();
        bool isPhaseOne = phaseNumber == SEARCH_PHASE_ONE;
        if (isPhaseOne) {
            metricsCollector->setRequestType(SessionMetricsCollector::IndependentPhase1);
        } else {
            metricsCollector->setRequestType(SessionMetricsCollector::IndependentPhase2);
        }

        Tensor* out = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &out));
        out->scalar<bool>()() = isPhaseOne;
    }
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, IsPhaseOneOp);

REGISTER_KERNEL_BUILDER(Name("IsPhaseOneOp")
                        .Device(DEVICE_CPU),
                        IsPhaseOneOp);

END_HA3_NAMESPACE(turing);
