#ifndef ISEARCH_AGGKERNEL_H
#define ISEARCH_AGGKERNEL_H

#include <ha3/sql/common/common.h>
#include <ha3/sql/data/Table.h>
#include <ha3/sql/ops/agg/AggBase.h>
#include <ha3/sql/proto/SqlSearchInfo.pb.h>
#include <navi/engine/N2OneKernel.h>
#include <ha3/sql/proto/SqlSearchInfoCollector.h>
#include <kmonitor/client/MetricsReporter.h>
#include <ha3/sql/ops/calc/CalcTable.h>

BEGIN_HA3_NAMESPACE(sql);

class AggKernel : public navi::N2OneKernel {
public:
    AggKernel();
private:
    AggKernel(const AggKernel &);
    AggKernel& operator=(const AggKernel &);
public:
    std::string inputPort() const override;
    std::string outputPort() const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode collect(const navi::DataPtr &data) override;
    navi::ErrorCode finalize(navi::DataPtr &data) override;
private:
    void patchHintInfo(const std::map<std::string, std::map<std::string, std::string> > &hintsMap);
    void reportMetrics();
    void incComputeTime();
    void incOutputCount(uint32_t count);
    void incTotalTime(int64_t time);
private:
    std::vector<std::string> _groupKeyVec;
    std::vector<std::string> _outputFields;
    std::vector<std::string> _outputFieldsType;
    std::vector<AggFuncDesc> _aggFuncDesc;
    AggHints _aggHints;
    std::string _scope;
    AggBasePtr _aggBase;
    kmonitor::MetricsReporter *_queryMetricsReporter = nullptr;
    AggInfo _aggInfo;
    SqlSearchInfoCollector *_sqlSearchInfoCollector = nullptr;
    AggFuncManagerPtr _aggFuncManager;
    int32_t _opId;
    CalcTablePtr _calcTable;
private:
    HA3_LOG_DECLARE();

};

HA3_TYPEDEF_PTR(AggKernel);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_AGGKERNEL_H
