#pragma once

#include <ha3/sql/ops/khronosScan/KhronosScanBase.h>
#include <ha3/sql/ops/khronosScan/KhronosMetaConditionVisitor.h>

BEGIN_HA3_NAMESPACE(sql);

class KhronosMetaScan : public KhronosScanBase {
private:
    enum KhronosScanType {
        KST_METRIC,
        KST_TAGK,
        KST_TAGV,
    };
public:
    KhronosMetaScan(const KhronosScanHints &scanHints = {},
                    KhronosScanAccessLog *accessLog = nullptr);
    virtual ~KhronosMetaScan();
private:
    KhronosMetaScan(const KhronosMetaScan&);
    KhronosMetaScan& operator=(const KhronosMetaScan &);
public:
    bool init(const ScanInitParam &param) override;
    bool doBatchScan(TablePtr &table, bool &eof) override;
private:
    bool initImpl(const ScanInitParam &param);
    bool doBatchScanImpl(TablePtr &table, bool &eof);
    bool fillScanType(const std::string &columnName);
    bool parseQueryParamsFromCondition();
    bool createSingleColumnTable(const std::set<std::string> &uniqueSet,
                                 const std::string &colName,
                                 size_t limit,
                                 TablePtr &table);
    std::string lookupParamsAsString();
    void reportInitFinishMetrics(bool success);
    void reportScanFinishMetrics(bool success);
private:
    std::shared_ptr<autil::mem_pool::Pool> _kernelPool;
    KhronosScanType _scanType;
    khronos::search::TimeRange _tsRange;
    khronos::search::SearchInterface::MetricMatchInfo _metric;
    std::string _tagk;
    std::string _tagv;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(KhronosMetaScan);

END_HA3_NAMESPACE(sql);
