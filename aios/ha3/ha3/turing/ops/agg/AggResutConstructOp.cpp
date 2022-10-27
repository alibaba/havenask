#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <iostream>
#include <ha3/common/ha3_op_common.h>
#include <ha3/util/Log.h>
#include <ha3/turing/variant/AggregateResultsVariant.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/common/Tracer.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/util/RangeUtil.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(turing);

class AggResultConstructOp : public tensorflow::OpKernel
{
public:
    explicit AggResultConstructOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
        ADD_OP_BASE_TAG(_tags);
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        auto aggResultsTensor = ctx->input(0).scalar<Variant>()();
        auto aggResultsVariant = aggResultsTensor.get<AggregateResultsVariant>();
        OP_REQUIRES(ctx, aggResultsVariant, errors::Unavailable("get aggregate results variant failed"));
        AggregateResultsPtr aggResults = aggResultsVariant->getAggResults();
        uint64_t aggCount = aggResultsVariant->getAggCount();
        uint64_t matchCount = aggResultsVariant->getToAggCount();
        uint64_t seekedCount = aggResultsVariant->getSeekedCount();
        uint64_t seekTermDocCount = aggResultsVariant->getSeekTermDocCount();
        common::ResultPtr result(new common::Result());
        result->fillAggregateResults(aggResults);
        result->setTracer(queryResource->getTracerPtr());
        Ha3MatchDocAllocatorPtr allocator(new Ha3MatchDocAllocator(queryResource->getPool()));
        MatchDocs *matchDocs = new MatchDocs();
        matchDocs->setMatchDocAllocator(allocator);
        result->setMatchDocs(matchDocs);
        result->setTotalMatchDocs(matchCount);
        result->setActualMatchDocs(matchCount);
        PhaseOneSearchInfo *searchInfo = new PhaseOneSearchInfo;
        searchInfo->partitionCount = 1;
        searchInfo->matchCount = matchCount;
        searchInfo->seekCount = seekedCount;
        searchInfo->seekDocCount = seekTermDocCount;
        result->setPhaseOneSearchInfo(searchInfo);
        const suez::ServiceInfo &serviceInfo = sessionResource->serviceInfo;
        string zoneBizName = serviceInfo.getZoneName()  + "." + sessionResource->bizName;
        util::PartitionRange utilRange;
        if (!RangeUtil::getRange(serviceInfo.getPartCount(), serviceInfo.getPartId(), utilRange)) {
            HA3_LOG(ERROR, "get Range failed");
        }
        result->addCoveredRange(zoneBizName, utilRange.first, utilRange.second);

        auto userMetricReporter = queryResource->getQueryMetricsReporter();
        if (likely(userMetricReporter != NULL)) {
            userMetricReporter->report(aggCount, "aggCount", kmonitor::GAUGE, &_tags);
            userMetricReporter->report(matchCount, "matchCount", kmonitor::GAUGE, &_tags);
        }

        Ha3ResultVariant ha3Result(result, queryResource->getPool());
        Tensor* out = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &out));
        out->scalar<Variant>()() = ha3Result;
    }

private:
    kmonitor::MetricsTags _tags;
private:
    HA3_LOG_DECLARE();

};

HA3_LOG_SETUP(turing, AggResultConstructOp);

REGISTER_KERNEL_BUILDER(Name("AggResultConstructOp")
                        .Device(DEVICE_CPU),
                        AggResultConstructOp);

END_HA3_NAMESPACE(turing);
