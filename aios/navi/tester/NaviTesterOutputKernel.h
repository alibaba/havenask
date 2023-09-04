#pragma once

#include "navi/engine/Kernel.h"
#include "navi/tester/NaviTesterResource.h"

namespace navi {

class NaviTesterOutputKernel : public Kernel
{
public:
    NaviTesterOutputKernel();
    ~NaviTesterOutputKernel();
    NaviTesterOutputKernel(const NaviTesterOutputKernel &) = delete;
    NaviTesterOutputKernel &operator=(const NaviTesterOutputKernel &) = delete;
public:
    void def(KernelDefBuilder &builder) const override;
    ErrorCode init(KernelInitContext &ctx) override;
    ErrorCode compute(KernelComputeContext &ctx) override;
private:
    NaviTesterResource *_testerResource = nullptr;
};

}
