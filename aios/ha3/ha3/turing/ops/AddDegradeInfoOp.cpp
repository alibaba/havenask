#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <iostream>
#include <ha3/common/ha3_op_common.h>
#include <suez/turing/common/SessionResource.h>
#include <ha3/util/Log.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/turing/ops/SearcherSessionResource.h>
#include <ha3/config/ServiceDegradationConfig.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(turing);

class AddDegradeInfoOp : public tensorflow::OpKernel
{
public:
    explicit AddDegradeInfoOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        SearcherSessionResource *searcherSessionResource =
            dynamic_cast<SearcherSessionResource *>(sessionResource);
        SearcherQueryResource *searcherQueryResource =
            dynamic_cast<SearcherQueryResource *>(queryResource);
        OP_REQUIRES(ctx, searcherSessionResource,
                    errors::Unavailable("ha3 searcher session resource unavailable"));
        OP_REQUIRES(ctx, searcherQueryResource,
                    errors::Unavailable("ha3 searcher query resource unavailable"));

        float serviceDegradationLevel = queryResource->getDegradeLevel();
        if (serviceDegradationLevel <= 0) {
            ctx->set_output(0, ctx->input(0));
            return;
        }

        HA3_NS(service)::SearcherResourcePtr searcherResource =
            searcherSessionResource->searcherResource;
        OP_REQUIRES(ctx, searcherResource,
                    errors::Unavailable("ha3 searcher resource unavailable"));

        HA3_NS(service)::ServiceDegradePtr serviceDegrade = searcherResource->getServiceDegrade();
        if (!serviceDegrade) {
            ctx->set_output(0,ctx->input(0));
            return;
        }

        HA3_NS(config)::ServiceDegradationConfig serviceDegradationConfig =
            serviceDegrade->getServiceDegradationConfig();
        uint32_t rankSize = serviceDegradationConfig.request.rankSize;
        uint32_t rerankSize = serviceDegradationConfig.request.rerankSize;

        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));
        request->setDegradeLevel(serviceDegradationLevel,rankSize,rerankSize);

        auto metricsCollector =  searcherQueryResource->sessionMetricsCollector;
        if (metricsCollector) {
            metricsCollector->setDegradeLevel(serviceDegradationLevel);
            metricsCollector->increaseServiceDegradationQps();
        }

        Tensor* out = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &out));
        out->scalar<Variant>()() = *ha3RequestVariant;
    }

private:
    HA3_LOG_DECLARE();

};

HA3_LOG_SETUP(turing, AddDegradeInfoOp);

REGISTER_KERNEL_BUILDER(Name("AddDegradeInfoOp")
                        .Device(DEVICE_CPU),
                        AddDegradeInfoOp);

END_HA3_NAMESPACE(turing);
