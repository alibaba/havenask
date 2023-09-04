#pragma once

#include "navi/engine/Kernel.h"
#include "navi/tester/NaviTesterResource.h"

namespace navi {

class NaviTesterInputKernel : public Kernel
{
public:
    NaviTesterInputKernel();
    ~NaviTesterInputKernel();
    NaviTesterInputKernel(const NaviTesterInputKernel &) = delete;
    NaviTesterInputKernel &operator=(const NaviTesterInputKernel &) = delete;
public:
    void def(KernelDefBuilder &builder) const override;
    ErrorCode init(KernelInitContext &ctx) override;
    ErrorCode compute(KernelComputeContext &ctx) override;
private:
    NaviTesterResource *_resource;
};

}
