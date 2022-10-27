#ifndef ISEARCH_AGGPREPAREOP_H
#define ISEARCH_AGGPREPAREOP_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <ha3/search/Aggregator.h>
#include <ha3/turing/variant/ExpressionResourceVariant.h>
#include <ha3/config/AggSamplerConfigInfo.h>
#include <basic_ops/util/OpUtil.h>

BEGIN_HA3_NAMESPACE(turing);

class AggPrepareOp : public tensorflow::OpKernel
{
public:
    explicit AggPrepareOp(tensorflow::OpKernelConstruction* ctx);
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;

    static search::AggregatorPtr createAggregator(const ExpressionResourcePtr &resource,
            tensorflow::QueryResource *queryResource,
            const config::AggSamplerConfigInfo &configInfo = config::AggSamplerConfigInfo());
private:
    config::AggSamplerConfigInfo _aggSamplerConfig;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_AGGPREPAREOP_H
