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

class Ha3NeedHobbitOp : public tensorflow::OpKernel
{
public:
    explicit Ha3NeedHobbitOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {     
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));
        bool needHobbit = false;
        auto finalSortClause = request->getFinalSortClause();
        if (finalSortClause && "AdapterSorter" == finalSortClause->getSortName()) {
            needHobbit = true;
        }
        
        Tensor* out = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &out));
        out->scalar<bool>()() = needHobbit;
    }
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, Ha3NeedHobbitOp);

REGISTER_KERNEL_BUILDER(Name("Ha3NeedHobbitOp")
                        .Device(DEVICE_CPU),
                        Ha3NeedHobbitOp);

END_HA3_NAMESPACE(turing);
