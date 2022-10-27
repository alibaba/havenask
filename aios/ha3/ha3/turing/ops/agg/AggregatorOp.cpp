#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <ha3/turing/variant/AggregatorVariant.h>
#include <ha3/common/ha3_op_common.h>

using namespace tensorflow;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(turing);

class AggregatorOp : public tensorflow::OpKernel
{
public:
    explicit AggregatorOp(tensorflow::OpKernelConstruction* ctx)
        : tensorflow::OpKernel(ctx)
    {
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto aggregatorTensor = ctx->input(0).scalar<Variant>()();
        auto aggregatorVariant = aggregatorTensor.get<AggregatorVariant>();
        OP_REQUIRES(ctx, aggregatorVariant, errors::Unavailable("get aggregator failed"));

        auto matchDocsTensor = ctx->input(1).scalar<Variant>()();
        auto matchDocsVariant = matchDocsTensor.get<MatchDocsVariant>();
        OP_REQUIRES(ctx, matchDocsVariant, errors::Unavailable("get matchDocs failed"));

        auto aggregator = aggregatorVariant->getAggregator();
        OP_REQUIRES(ctx, aggregator, errors::Unavailable("aggregator is empty."));

        aggregator->batchAggregate(matchDocsVariant->getMatchDocs());
        ctx->set_output(0, ctx->input(0));
        ctx->set_output(1, ctx->input(1));
    }
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, AggregatorOp);

REGISTER_KERNEL_BUILDER(Name("AggregatorOp")
                        .Device(DEVICE_CPU),
                        AggregatorOp);

END_HA3_NAMESPACE(turing);
