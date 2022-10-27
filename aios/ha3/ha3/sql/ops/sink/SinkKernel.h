#ifndef ISEARCH_SINK_KERNEL_H
#define ISEARCH_SINK_KERNEL_H

#include <ha3/sql/common/common.h>
#include <ha3/sql/data/Table.h>
#include <navi/engine/Kernel.h>

BEGIN_HA3_NAMESPACE(sql);

class SinkKernel : public navi::Kernel {
public:
    SinkKernel();
    ~SinkKernel();
public:
    const navi::KernelDef *getDef() const override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;
private:
    void outputResult(navi::KernelComputeContext &runContext);
    bool doCompute(const navi::DataPtr &data);
private:
    TablePtr _table;
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SinkKernel);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_SINK_KERNEL_H
