#pragma once

#include <ha3/sql/common/common.h>
#include <ha3/sql/data/Table.h>
#include <navi/engine/Kernel.h>
#include <navi/resource/MemoryPoolResource.h>
#include <ha3/sql/proto/SqlSearchInfo.pb.h>
#include <ha3/sql/proto/SqlSearchInfoCollector.h>
#include <ha3/sql/ops/tvf/TvfFuncManager.h>
#include <kmonitor/client/MetricsReporter.h>
BEGIN_HA3_NAMESPACE(sql);

class InvocationAttr : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool parseStrParams(std::vector<std::string> &params, std::vector<std::string> &inputTables);
public:
    std::string tvfName;
    std::string tvfParams;
    std::string type;
private:
    HA3_LOG_DECLARE();
};

class TvfKernel : public navi::Kernel {
public:
    TvfKernel();
    ~TvfKernel();
public:
    const navi::KernelDef *getDef() const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;
private:
    bool checkOutputTable(const TablePtr &table);
    void incComputeTimes();
    void incOutputCount(int64_t count);
    void incTotalTime(int64_t time);
    void reportMetrics();

private:
    std::vector<std::string> _outputFields;
    std::vector<std::string> _outputFieldsType;
    KernelScope _scope;
    InvocationAttr _invocation;
    HA3_NS(sql)::TvfFuncManager *_tvfFuncManager;
    kmonitor::MetricsReporter *_queryMetricsReporter;
    SqlSearchInfoCollector *_sqlSearchInfoCollector;
    autil::mem_pool::Pool *_queryPool;
    navi::MemoryPoolResource *_poolResource;
    TvfInfo _tvfInfo;
    TvfFuncPtr _tvfFunc;
    int32_t _opId;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(TvfKernel);

END_HA3_NAMESPACE(sql);
