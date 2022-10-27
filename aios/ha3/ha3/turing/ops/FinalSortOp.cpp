#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <iostream>
#include <ha3/common/ha3_op_common.h>
#include <suez/turing/common/SessionResource.h>
#include <ha3/common.h>
#include <ha3/util/Log.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(turing);

/*REGISTER_OP("FinalSortOp")
.Input("integers: int32")
//.Output("matchdocs: variant");
.Output("result: int64");*/

class FinalSortOp : public tensorflow::OpKernel
{
public:
    explicit FinalSortOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        Tensor* out = nullptr;

        MatchDocsVariant matchDocs;
        for (auto i = 0; i < 10; i++) {
            matchDocs.allocate(i);
        }

        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {1}, &out));
        out->scalar<Variant>()() = matchDocs;
        }

private:
    HA3_LOG_DECLARE();

};

HA3_LOG_SETUP(turing, FinalSortOp);

REGISTER_KERNEL_BUILDER(Name("FinalSortOp")
                        .Device(DEVICE_CPU),
                        FinalSortOp);

END_HA3_NAMESPACE(turing);
