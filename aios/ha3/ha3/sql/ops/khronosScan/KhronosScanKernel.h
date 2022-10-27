#pragma once

#include <ha3/sql/common/common.h>
#include <navi/engine/Kernel.h>
#include <ha3/sql/ops/khronosScan/KhronosScanBase.h>
#include <ha3/sql/ops/khronosScan/KhronosScanAccessLog.h>

BEGIN_HA3_NAMESPACE(sql);

class KhronosScanKernel : public navi::Kernel {
public:
    KhronosScanKernel();
    ~KhronosScanKernel();
private:
    KhronosScanKernel(const KhronosScanKernel&);
    KhronosScanKernel& operator=(const KhronosScanKernel &);
public:
    const navi::KernelDef *getDef() const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;
private:
    void patchHintInfo(const std::map<std::string, std::map<std::string, std::string> > &hintsMap);
private:
    KhronosScanAccessLogPtr _accessLog;
    ScanInitParam _initParam;
    KhronosScanHints _scanHints;
    KhronosScanBasePtr _scanBase;
private:
    HA3_LOG_DECLARE();

};

HA3_TYPEDEF_PTR(KhronosScanKernel);

END_HA3_NAMESPACE(sql);
