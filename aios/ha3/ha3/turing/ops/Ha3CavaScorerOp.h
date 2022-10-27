#pragma once
#include <ha3/common.h>
#include <ha3/common/ha3_op_common.h>
#include <ha3/util/Log.h>
#include <autil/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <basic_ops/ops/basic/scorer_op/CavaScorerOp.h>
#include <suez/turing/expression/util/ScoringProviderBuilder.h>

BEGIN_HA3_NAMESPACE(turing);

class Ha3CavaScorerOp : public suez::turing::CavaScorerOp {
public:
    explicit Ha3CavaScorerOp(tensorflow::OpKernelConstruction* ctx)
        : CavaScorerOp(ctx)
    {
    }

protected:
    suez::turing::ScoringProviderBuilderPtr createScoringProviderBuilder() override;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(turing);
