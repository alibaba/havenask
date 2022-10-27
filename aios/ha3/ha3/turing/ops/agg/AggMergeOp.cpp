#ifndef ISEARCH_HA3AGGMERGEOP_H
#define ISEARCH_HA3AGGMERGEOP_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/proxy/Merger.h>
#include <ha3/common/ha3_op_common.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <ha3/turing/variant/AggregatorVariant.h>
#include <ha3/turing/variant/AggregateResultsVariant.h>
using namespace tensorflow;
using namespace suez::turing;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(turing);

class AggMergeOp : public tensorflow::OpKernel {
public:
    explicit AggMergeOp(tensorflow::OpKernelConstruction* ctx)
        : tensorflow::OpKernel(ctx)
    {}
public:
    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        OpInputList aggregators;
        OP_REQUIRES_OK(ctx, ctx->input_list("aggregators", &aggregators));
        const int n = aggregators.size();
        std::vector<common::AggregateResults> results;
        AggregateClause *aggClause = NULL;
        uint64_t aggCount = 0;
        uint64_t toAggCount = 0;
        uint64_t seekedCount = 0;
        uint64_t seekTermDocCount = 0;
        for (int i = 0; i < n; i++) {
            auto aggregatorVariant = aggregators[i].scalar<Variant>()().get<AggregatorVariant>();
            if (!aggregatorVariant) {
                continue;
            }
            auto aggregator = aggregatorVariant->getAggregator();
            if (!aggregator) {
                continue;
            }
            seekedCount += aggregatorVariant->getSeekedCount();
            seekTermDocCount += aggregatorVariant->getSeekTermDocCount();
            aggregator->endLayer(1.0);
            aggregator->estimateResult(1.0);
            aggCount += aggregator->getAggregateCount();
            toAggCount += aggregator->getToAggDocCount();
            if (!aggClause) {
                ExpressionResourcePtr resource = aggregatorVariant->getExpressionResource();
                aggClause = resource->_request->getAggregateClause();
            }
            auto aggregateResultsPtr = aggregator->collectAggregateResult();
            results.push_back(*aggregateResultsPtr);
        }

        // merge
        common::AggregateResultsPtr aggResultsPtr(new AggregateResults());
        if (results.size() == 1) {
            *aggResultsPtr = results[0];
        } else if (results.size() > 1) {
            proxy::Merger::mergeAggResults(*aggResultsPtr, results, aggClause, queryResource->getPool());
        }
        // output
        AggregateResultsVariant aggResultsVariant(aggResultsPtr, toAggCount,
                aggCount, queryResource->getPool());
        aggResultsVariant.setSeekedCount(seekedCount);
        aggResultsVariant.setSeekTermDocCount(seekTermDocCount);
        Tensor *outputTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, TensorShape({}), &outputTensor));
        outputTensor->scalar<Variant>()() = aggResultsVariant;
    }
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, AggMergeOp);

REGISTER_KERNEL_BUILDER(Name("AggMergeOp")
                        .Device(DEVICE_CPU),
                        AggMergeOp);

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_HA3AGGMERGEOP_H
