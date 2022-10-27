#pragma once
#include "ha3/common.h"
#include "ha3/isearch.h"
#include "ha3/util/Log.h"
#include <tensorflow/core/framework/op_kernel.h>
#include "ha3/turing/variant/LayerMetasVariant.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "basic_ops/util/OpUtil.h"

BEGIN_HA3_NAMESPACE(turing);

class Ha3SeekParaSplitOp : public tensorflow::OpKernel
{
public:
    explicit Ha3SeekParaSplitOp(tensorflow::OpKernelConstruction* ctx)
        : tensorflow::OpKernel(ctx)
    {
        OP_REQUIRES_OK(ctx, ctx->GetAttr("N", &_outputNum));
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;
private:
    int32_t _outputNum;
private:
    HA3_LOG_DECLARE();
};
END_HA3_NAMESPACE(turing);
