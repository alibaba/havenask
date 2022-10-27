#include <ha3/common/ha3_op_common.h>
#include <ha3/common.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <basic_variant/variant/KvPairVariant.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(turing);

class Ha3RequestToKvPairOp : public tensorflow::OpKernel
{
public:
    explicit Ha3RequestToKvPairOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        CHECK_TIMEOUT(queryResource);

        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto requestVariant = requestTensor.get<Ha3RequestVariant>();
        OP_REQUIRES(ctx, requestVariant, errors::Unavailable("get requestVariant failed"));
        RequestPtr request = requestVariant->getRequest();
        ConfigClause *configClause = request->getConfigClause();
        OP_REQUIRES(ctx, configClause, errors::Unavailable("configClause is nullptr"));
        KeyValueMapPtr kvPairs = configClause->getKeyValueMapPtr();
        OP_REQUIRES(ctx, kvPairs, errors::Unavailable("KeyValueMap is nullptr"));
        KvPairVariant kvPairVariant(kvPairs, queryResource->getPool());

        Tensor* tensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &tensor));
        tensor->scalar<Variant>()() = move(kvPairVariant);
    }
};

REGISTER_KERNEL_BUILDER(Name("Ha3RequestToKvPairOp")
                        .Device(DEVICE_CPU),
                        Ha3RequestToKvPairOp);

END_HA3_NAMESPACE(turing);
