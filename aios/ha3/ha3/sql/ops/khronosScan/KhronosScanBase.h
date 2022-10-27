#pragma once

#include <ha3/sql/ops/scan/ScanBase.h>
#include <ha3/sql/ops/khronosScan/KhronosCommon.h>
#include <ha3/sql/ops/khronosScan/KhronosScanAccessLog.h>
#include <ha3/sql/ops/condition/ConditionVisitor.h>
#include <indexlib/partition/index_partition_reader.h>
#include <khronos_table_interface/SearchInterface.h>

BEGIN_HA3_NAMESPACE(sql);

struct KhronosScanHints {
    KhronosScanHints()
        : oneLineScanPointLimit(KHR_DEFAULT_ONE_LINE_SCAN_POINT_LIMIT)
    {}
    size_t oneLineScanPointLimit;
};

class KhronosScanBase : public ScanBase {
public:
    KhronosScanBase(const KhronosScanHints &scanHints, KhronosScanAccessLog *accessLog)
        : _scanHints(scanHints)
        , _accessLog(accessLog)
    {
        _enableTableInfo = false;
    }
    virtual ~KhronosScanBase() {}
public:
    virtual bool init(const ScanInitParam &param) = 0;
protected:
    bool visitAndParseCondition(ConditionVisitor *visitor);
    void reportSearchMetrics(bool success, const khronos::search::SearchMetric &searchMetric);
    std::string getTableNameForMetrics() override {
        return _catalogName;
    }
    void reportSimpleQps(const std::string &metricName, kmonitor::MetricsTags tags = {});
    template <typename MetricsGroupT, typename MetricsValueT>
    void reportUserMetrics(kmonitor::MetricsTags tags, MetricsValueT *value);
private:
    static khronos::search::SearchInterfacePtr getKhronosTableInfo(
            IE_NAMESPACE(partition)::PartitionReaderSnapshot *partitionReaderSnapshot,
            const std::string &tableName);
protected:
    KhronosScanHints _scanHints;
    KhronosScanAccessLog *_accessLog;
    kmonitor::MetricsReporterPtr _opMetricsReporter;
    khronos::search::SearchInterfacePtr _tableReader;
private:
    HA3_LOG_DECLARE();
};

template <typename MetricsGroupT, typename MetricsValueT>
void KhronosScanBase::reportUserMetrics(kmonitor::MetricsTags tags, MetricsValueT *value) {
    if (_opMetricsReporter != nullptr) {
        tags.AddTag("catalog", _catalogName);
        _opMetricsReporter->report<MetricsGroupT>(&tags, value);
    }
}


#define ON_KHRONOS_SCAN_TERMINATOR_TIMEOUT(hint)        \
    SQL_LOG(ERROR, hint", left=[%ld], expire=[%ld]",    \
            _timeoutTerminator->getLeftTime(),          \
            _timeoutTerminator->getExpireTime())

HA3_TYPEDEF_PTR(KhronosScanBase);

END_HA3_NAMESPACE(sql);
