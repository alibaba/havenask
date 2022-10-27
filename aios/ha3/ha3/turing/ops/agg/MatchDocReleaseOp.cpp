#ifndef ISEARCH_MATCHDOCRELEASEOP_H
#define ISEARCH_MATCHDOCRELEASEOP_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <ha3/common/ha3_op_common.h>

using namespace tensorflow;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(turing);

class MatchDocReleaseOp : public tensorflow::OpKernel
{
public:
    explicit MatchDocReleaseOp(tensorflow::OpKernelConstruction* ctx)
        : tensorflow::OpKernel(ctx)
    {
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto matchDocsTensor = ctx->input(1).scalar<Variant>()();
        auto matchDocsVariant = matchDocsTensor.get<MatchDocsVariant>();
        OP_REQUIRES(ctx, matchDocsVariant, errors::Unavailable("get matchDocs failed"));
        std::vector<matchdoc::MatchDoc> &matchDocs = matchDocsVariant->getMatchDocs();
        auto allocator = matchDocsVariant->getAllocator();
        for (size_t i = 0; i < matchDocs.size(); ++i) {
            allocator->deallocate(matchDocs[i]);
        }
        matchDocs.clear();
        bool eof = ctx->input(0).scalar<bool>()();
        HA3_LOG(WARN, "seek end [%d]", eof);

        ctx->set_output(0, ctx->input(0));
    }
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, MatchDocReleaseOp);

REGISTER_KERNEL_BUILDER(Name("MatchDocReleaseOp")
                        .Device(DEVICE_CPU),
                        MatchDocReleaseOp);

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_MATCHDOCRELEASEOP_H
