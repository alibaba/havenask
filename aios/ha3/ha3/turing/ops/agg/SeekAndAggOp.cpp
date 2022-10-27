#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <ha3/common/ha3_op_common.h>
#include <suez/turing/common/SessionResource.h>
#include <ha3/util/Log.h>
#include <ha3/search/SingleLayerSearcher.h>
#include <ha3/turing/variant/SeekIteratorVariant.h>
#include <ha3/turing/variant/AggregatorVariant.h>
#include <matchdoc/MatchDocAllocator.h>

using namespace std;
using namespace tensorflow;
using namespace matchdoc;
using namespace suez::turing;

USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(turing);

class SeekAndAggOp : public tensorflow::OpKernel
{
public:
    explicit SeekAndAggOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
        OP_REQUIRES_OK(ctx, ctx->GetAttr("batch_size", &_batchSize));
        _seekCountName = "user.op.seekCount";
        ADD_OP_BASE_TAG(_tags);
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);

        auto aggVariant = ctx->input(0).scalar<Variant>()().get<AggregatorVariant>();
        OP_REQUIRES(ctx, aggVariant, errors::Unavailable("get agg variant failed"));

        auto seekIteratorVariant = ctx->input(1).scalar<Variant>()().get<SeekIteratorVariant>();
        OP_REQUIRES(ctx, seekIteratorVariant, errors::Unavailable("get seek iterator variant failed"));
        SeekIteratorPtr seekIterator = seekIteratorVariant->getSeekIterator();
        if (!seekIterator) {
            ctx->set_output(0, ctx->input(0));
            return;
        }
        ExpressionResourcePtr resource = aggVariant->getExpressionResource();
        OP_REQUIRES(ctx, resource, errors::Unavailable("get expression resource failed"));
        RequestPtr request = resource->_request;
        AggregatorPtr aggregator = aggVariant->getAggregator();
        OP_REQUIRES(ctx, aggregator, errors::Unavailable("get aggregator failed"));
        int32_t rankSize = getRankSize(request);
        uint32_t seekCount = doSeekAndAgg(seekIterator, aggregator, rankSize);
        auto userMetricReporter = queryResource->getQueryMetricsReporter();
        if (likely(userMetricReporter != NULL)) {
            userMetricReporter->report(seekCount, _seekCountName, kmonitor::GAUGE, &_tags);
        }
        AggregatorVariant newAggVariant = *aggVariant;
        newAggVariant.setSeekedCount(seekIterator->getSeekTimes());
        newAggVariant.setSeekTermDocCount(seekIterator->getSeekDocCount());

        Tensor *outputTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, TensorShape({}), &outputTensor));
        outputTensor->scalar<Variant>()() = newAggVariant;
    }

private:
    int32_t getRankSize(const RequestPtr &request) {
        int32_t rankSize = 0;
        auto configClause = request->getConfigClause();
        if (configClause) {
            rankSize = configClause->getRankSize();
        }
        if (rankSize == 0) {
            rankSize = numeric_limits<int32_t>::max();
        }
        return rankSize;
    }
    uint32_t doSeekAndAgg(const SeekIteratorPtr &seekIterator,
                      const AggregatorPtr &aggregator,
                      int32_t rankSize)
    {
        uint32_t seekCount = rankSize;
        MatchDocAllocatorPtr allocator = seekIterator->getMatchDocAllocator();
        bool eof = false;
        if (_batchSize > 1) {
            int32_t size = 0;
            vector<matchdoc::MatchDoc> matchDocs;
            matchDocs.reserve(_batchSize);
            while (rankSize > 0 && !eof) {
                size = rankSize < _batchSize ? rankSize : _batchSize;
                eof = seekIterator->batchSeek(size, matchDocs);
                aggregator->batchAggregate(matchDocs);
                rankSize -= matchDocs.size();
                for (auto matchDoc : matchDocs) {
                    allocator->deallocate(matchDoc);
                }
                matchDocs.clear();
            }
        } else {
            while (rankSize > 0) {
                matchdoc::MatchDoc doc = seekIterator->seek();
                if (matchdoc::INVALID_MATCHDOC == doc) {
                    break;
                }
                aggregator->aggregate(doc);
                allocator->deallocate(doc);
                rankSize--;
            }
        }
        return seekCount - rankSize;
    }

private:
    HA3_LOG_DECLARE();
private:
private:
    kmonitor::MetricsTags _tags;
    string _seekCountName;
    int _batchSize;
};

HA3_LOG_SETUP(turing, SeekAndAggOp);

REGISTER_KERNEL_BUILDER(Name("SeekAndAggOp")
                        .Device(DEVICE_CPU),
                        SeekAndAggOp);

END_HA3_NAMESPACE(turing);
