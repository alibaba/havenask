#ifndef ISEARCH_SQL_SORT_KERNEL_H
#define ISEARCH_SQL_SORT_KERNEL_H

#include <ha3/sql/common/common.h>
#include <ha3/sql/data/Table.h>
#include <ha3/sql/rank/ComboComparator.h>
#include <navi/engine/Kernel.h>
#include <ha3/sql/proto/SqlSearchInfo.pb.h>
#include <ha3/sql/proto/SqlSearchInfoCollector.h>
#include <ha3/util/Log.h>
#include <kmonitor/client/MetricsReporter.h>

BEGIN_HA3_NAMESPACE(sql);

class SortKernel : public navi::Kernel {
public:
    SortKernel();
    ~SortKernel();
private:
    SortKernel(const SortKernel &);
    SortKernel& operator=(const SortKernel &);
public:
    const navi::KernelDef *getDef() const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;
private:
    void outputResult(navi::KernelComputeContext &runContext);
    bool doLimitCompute(const navi::DataPtr &data);
    bool doCompute(const navi::DataPtr &data);
    void reportMetrics();
    void incComputeTime();
    void incMergeTime(int64_t time);
    void incCompactTime(int64_t time);
    void incTopKTime(int64_t time);
    void incOutputTime(int64_t time);
    void incTotalTime(int64_t time);
private:
    size_t _limit;
    size_t _offset;
    size_t _topk;
    TablePtr _table;
    ComboComparatorPtr _comparator;
    std::vector<std::string> _keys;
    std::vector<bool> _orders;
    autil::mem_pool::Pool *_pool;
    kmonitor::MetricsReporter *_queryMetricsReporter;
    SortInfo _sortInfo;
    SqlSearchInfoCollector *_sqlSearchInfoCollector;
    int32_t _opId;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SortKernel);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_SQL_SORT_KERNEL_H
