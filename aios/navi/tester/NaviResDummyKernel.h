#pragma once
#include "navi/engine/Kernel.h"

namespace navi {

class NaviResDummyKernel : public Kernel {
public:
    void def(KernelDefBuilder &builder) const override {}
    bool config(KernelConfigContext &ctx) override {
        NAVI_KERNEL_LOG(DEBUG, "start config kernel");
        return true;
    }
    ErrorCode init(KernelInitContext &ctx) override {
        NAVI_KERNEL_LOG(DEBUG, "start init kernel");
        return EC_NONE;
    }
    ErrorCode compute(KernelComputeContext &ctx) override {
        NAVI_KERNEL_LOG(DEBUG, "start compute kernel");
        return EC_NONE;
    }
};

} // namespace navi
