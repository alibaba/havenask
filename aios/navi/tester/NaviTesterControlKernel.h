#pragma once

#include "navi/engine/Kernel.h"

namespace navi {

class NaviTesterResource;

class NaviTesterControlKernel : public Kernel
{
public:
    NaviTesterControlKernel();
    ~NaviTesterControlKernel();
    NaviTesterControlKernel(const NaviTesterControlKernel &) = delete;
    NaviTesterControlKernel &operator=(const NaviTesterControlKernel &) = delete;
public:
    void def(KernelDefBuilder &builder) const override;
    ErrorCode init(KernelInitContext &ctx) override;
    ErrorCode compute(KernelComputeContext &ctx) override;
private:
    NaviTesterResource *_testerResource = nullptr;
};

}
