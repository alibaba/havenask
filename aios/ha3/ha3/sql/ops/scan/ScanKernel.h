#ifndef ISEARCH_SQL_SCAN_KERNEL_H
#define ISEARCH_SQL_SCAN_KERNEL_H

#include <ha3/sql/common/common.h>
#include <navi/engine/Kernel.h>
#include <ha3/sql/ops/scan/ScanBase.h>
BEGIN_HA3_NAMESPACE(sql);

class ScanKernel : public navi::Kernel {
public:
    ScanKernel();
    ~ScanKernel();
private:
    ScanKernel(const ScanKernel&);
    ScanKernel& operator=(const ScanKernel &);
public:
    const navi::KernelDef *getDef() const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;
private:
    ScanInitParam _initParam;
    ScanBasePtr _scanBase;
private:
    HA3_LOG_DECLARE();

};

HA3_TYPEDEF_PTR(ScanKernel);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_SQL_SCAN_KERNEL_H
