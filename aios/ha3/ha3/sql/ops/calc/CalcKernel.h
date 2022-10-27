#ifndef ISEARCH_CALC_KERNEL_H
#define ISEARCH_CALC_KERNEL_H

#include <ha3/sql/common/common.h>
#include <ha3/sql/data/Table.h>
#include <navi/engine/Kernel.h>
#include <navi/resource/MemoryPoolResource.h>
#include <ha3/sql/ops/condition/Condition.h>
#include <ha3/sql/proto/SqlSearchInfo.pb.h>
#include <ha3/sql/ops/calc/CalcTable.h>
#include <kmonitor/client/MetricsReporter.h>

BEGIN_HA3_NAMESPACE(sql);

class CalcKernel : public navi::Kernel {
public:
    CalcKernel();
    ~CalcKernel();
public:
    const navi::KernelDef *getDef() const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;
private:
    void incComputeTimes();
    void incOutputCount(int64_t count);
    void incTotalTime(int64_t time);
    void reportMetrics();
private:
    std::string _conditionJson;
    std::string _outputExprsJson;
    ConditionPtr _condition;
    std::vector<std::string> _outputFields;
    std::vector<std::string> _outputFieldsType;
    CalcInfo _calcInfo;
    CalcTablePtr _calcTable;
    kmonitor::MetricsReporter *_queryMetricsReporter;
    SqlSearchInfoCollector *_sqlSearchInfoCollector;
    int32_t _opId;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(CalcKernel);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_CALC_KERNEL_H
