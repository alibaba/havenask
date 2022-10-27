#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <ha3/common/ha3_op_common.h>
#include <suez/turing/common/SessionResource.h>
#include <ha3/util/Log.h>
#include <ha3/search/SingleLayerSearcher.h>
#include <ha3/turing/variant/SeekIteratorVariant.h>
#include <ha3/common/ha3_op_common.h>
#include <matchdoc/MatchDocAllocator.h>

using namespace std;
using namespace tensorflow;
using namespace matchdoc;
using namespace suez::turing;

USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(turing);

class SeekOp : public tensorflow::OpKernel
{
public:
    explicit SeekOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
        OP_REQUIRES_OK(ctx, ctx->GetAttr("batch_size", &_batchSize));
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        auto seekIteratorVariant = ctx->input(0).scalar<Variant>()().get<SeekIteratorVariant>();
        OP_REQUIRES(ctx, seekIteratorVariant, errors::Unavailable("get seek iterator variant failed"));
        SeekIteratorPtr seekIterator = seekIteratorVariant->getSeekIterator();
        if (!seekIterator) {
            ctx->set_output(0, ctx->input(0));
            Tensor *matchDocsTensor = nullptr;
            MatchDocsVariant matchDocsVariant;
            OP_REQUIRES_OK(ctx, ctx->allocate_output(1, {}, &matchDocsTensor));
            matchDocsTensor->scalar<Variant>()() = move(matchDocsVariant);

            Tensor *eofTensor = nullptr;
            OP_REQUIRES_OK(ctx, ctx->allocate_output(2, {}, &eofTensor));
            eofTensor->scalar<bool>()() = true;
            return;
        }

        MatchDocAllocatorPtr allocator = seekIterator->getMatchDocAllocator();

        MatchDocsVariant matchDocsVariant(allocator, queryResource->getPool());
        vector<MatchDoc> &seekDocs = matchDocsVariant.getMatchDocs();
        bool eof = seekMatchDocs(seekIterator.get(), _batchSize, seekDocs);
        ctx->set_output(0, ctx->input(0));

        Tensor *matchDocsTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(1, {}, &matchDocsTensor));
        matchDocsTensor->scalar<Variant>()() = move(matchDocsVariant);

        Tensor *eofTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(2, {}, &eofTensor));
        eofTensor->scalar<bool>()() = eof;
  }
private:
    bool seekMatchDocs(SeekIterator *seekIterator, int batchSize, vector<MatchDoc> &seekDocs);

private:
    int _batchSize;
private:
    HA3_LOG_DECLARE();

};

bool SeekOp::seekMatchDocs(SeekIterator *seekIterator, int batchSize, vector<MatchDoc> &seekDocs) {
    if (batchSize <= 0) {
        while (true) {
            MatchDoc matchDoc = seekIterator->seek();
            if (matchdoc::INVALID_MATCHDOC == matchDoc) {
                return true;
            }
            seekDocs.push_back(matchDoc);
        }
    } else {
        return seekIterator->batchSeek(batchSize, seekDocs);
    }
}


HA3_LOG_SETUP(turing, SeekOp);

REGISTER_KERNEL_BUILDER(Name("SeekOp")
                        .Device(DEVICE_CPU),
                        SeekOp);

END_HA3_NAMESPACE(turing);
