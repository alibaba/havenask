#include <ha3/common/ha3_op_common.h>
#include <ha3/common.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <ha3/turing/ops/SearcherQueryResource.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(turing);

class ErrorResultMergeOp : public tensorflow::OpKernel
{
public:
    explicit ErrorResultMergeOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
        OP_REQUIRES_OK(ctx, ctx->GetAttr("error_code", &_errorCode));
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        CHECK_TIMEOUT(queryResource);
        SearcherQueryResource *searcherQueryResource =
            dynamic_cast<SearcherQueryResource *>(queryResource);
        OP_REQUIRES(ctx, searcherQueryResource,
                    errors::Unavailable("ha3 searcher query resource unavailable"));
        auto commonResource = searcherQueryResource->commonResource;
        OP_REQUIRES(ctx, commonResource,
                    errors::Unavailable("ha3 common resource unavailable"));

        string errorInfo = ctx->input(0).scalar<string>()();
        if (!errorInfo.empty()) {
            commonResource->errorResult->addError(_errorCode, errorInfo);
        }
    }
private:
    int32_t _errorCode;
};

REGISTER_KERNEL_BUILDER(Name("ErrorResultMergeOp")
                        .Device(DEVICE_CPU),
                        ErrorResultMergeOp);

END_HA3_NAMESPACE(turing);
