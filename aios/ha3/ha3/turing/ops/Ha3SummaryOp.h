#pragma once
#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <iostream>
#include <ha3/common/ha3_op_common.h>
#include <suez/turing/common/SessionResource.h>
#include <ha3/util/Log.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/turing/ops/SearcherSessionResource.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/search/SummarySearcher.h>
#include <ha3/search/SearchCommonResource.h>
#include <ha3/service/SearcherResource.h>
#include <indexlib/partition/index_partition.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <ha3/common/Tracer.h>
#include <ha3/common/TimeoutTerminator.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/proto/ProtoClassUtil.h>

BEGIN_HA3_NAMESPACE(turing);

class Ha3SummaryOp : public tensorflow::OpKernel
{
public:
    explicit Ha3SummaryOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
        OP_REQUIRES_OK(ctx, ctx->GetAttr("force_allow_lack_of_summary", &_forceAllowLackOfSummary));
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;

    common::ResultPtr processPhaseTwoRequest(const common::Request *request,
            service::SearcherResourcePtr resourcePtr,
            SearcherQueryResource *queryResource);
    void addTraceInfo(monitor::SessionMetricsCollector *metricsCollector, suez::turing::Tracer* tracer);
private:
    bool _forceAllowLackOfSummary = false;
private:
    HA3_LOG_DECLARE();

};
END_HA3_NAMESPACE(turing);
