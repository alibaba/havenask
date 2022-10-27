#ifndef ISEARCH_MERGE_KERNEL_H
#define ISEARCH_MERGE_KERNEL_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/data/Table.h>
#include <navi/engine/Kernel.h>

BEGIN_HA3_NAMESPACE(sql);

class MergeKernel : public navi::Kernel {
public:
    MergeKernel();
    ~MergeKernel();
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

HA3_TYPEDEF_PTR(MergeKernel);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_MERGE_KERNEL_H
