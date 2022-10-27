#ifndef ISEARCH_UNION_KERNEL_H
#define ISEARCH_UNION_KERNEL_H

#include <ha3/sql/common/common.h>
#include <ha3/sql/data/Table.h>
#include <navi/engine/Kernel.h>

BEGIN_HA3_NAMESPACE(sql);

class UnionKernel : public navi::Kernel {
public:
    UnionKernel();
    ~UnionKernel();
public:
    const navi::KernelDef *getDef() const override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;
private:
    void outputResult(navi::KernelComputeContext &runContext);
    bool doCompute(const navi::DataPtr &data);
private:
    TablePtr _table;
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(UnionKernel);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_UNION_KERNEL_H
