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
#include <ha3/common/TimeoutTerminator.h>
#include <ha3/proto/ProtoClassUtil.h>
#include <ha3/common/Tracer.h>

BEGIN_HA3_NAMESPACE(turing);

class Ha3RequestInitOp : public tensorflow::OpKernel
{
public:
    explicit Ha3RequestInitOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {};
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;

private:
    void addEagleTraceInfo(multi_call::QuerySession *querySession, HA3_NS(common)::Tracer *tracer) ;
    HA3_NS(common)::TimeoutTerminatorPtr createTimeoutTerminator(const HA3_NS(common)::RequestPtr &request,
            int64_t startTime, int64_t currentTime, bool isSeekTimeout);

private:
    static const int32_t MS_TO_US_FACTOR = 1000;
    HA3_LOG_DECLARE();
};


END_HA3_NAMESPACE(turing);
