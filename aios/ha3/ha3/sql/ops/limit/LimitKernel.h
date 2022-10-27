#ifndef ISEARCH_SQL_LIMIT_KERNEL_H
#define ISEARCH_SQL_LIMIT_KERNEL_H

#include <ha3/sql/common/common.h>
#include <ha3/sql/data/Table.h>
#include <navi/engine/Kernel.h>
#include <ha3/util/Log.h>
#include <kmonitor/client/MetricsReporter.h>

BEGIN_HA3_NAMESPACE(sql);

class LimitKernel : public navi::Kernel {
public:
    LimitKernel();
    ~LimitKernel();
private:
    LimitKernel(const LimitKernel &);
    LimitKernel& operator=(const LimitKernel &);
public:
    const navi::KernelDef *getDef() const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;
private:
    void outputResult(navi::KernelComputeContext &runContext);
    bool doCompute(const navi::DataPtr &data);
    void reportMetrics();
private:
    size_t _limit;
    size_t _offset;
    size_t _topk;
    TablePtr _table;
    kmonitor::MetricsReporter *_queryMetricsReporter;
    size_t _outputCount;

private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(LimitKernel);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_SQL_LIMIT_KERNEL_H
