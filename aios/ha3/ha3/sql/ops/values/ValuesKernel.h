#ifndef ISEARCH_VALUES_KERNEL_H
#define ISEARCH_VALUES_KERNEL_H

#include <ha3/sql/common/common.h>
#include <ha3/util/Log.h>
#include <navi/engine/Kernel.h>
#include <navi/resource/MemoryPoolResource.h>

BEGIN_HA3_NAMESPACE(sql);

class ValuesKernel : public navi::Kernel {
public:
    ValuesKernel();
    ~ValuesKernel();
public:
    const navi::KernelDef *getDef() const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;
private:
    std::vector<std::string> _outputFields;
    std::vector<std::string> _outputTypes;
    navi::MemoryPoolResource *_memoryPoolResource;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ValuesKernel);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_VALUES_KERNEL_H
