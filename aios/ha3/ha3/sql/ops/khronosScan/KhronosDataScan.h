#pragma once

#include <ha3/sql/ops/khronosScan/KhronosScanBase.h>
#include <ha3/sql/ops/khronosScan/KhronosDataConditionVisitor.h>
#include <ha3/sql/ops/condition/OutputFieldsVisitor.h>
#include <ha3/sql/ops/khronosScan/KhronosCommon.h>
#include <ha3/sql/ops/khronosScan/proto/khronosSearchInfo.pb.h>
#include <ha3/sql/ops/calc/CalcTable.h>
#include <khronos_table_interface/CommonDefine.h>
#include <khronos_table_interface/LineSegmentBuffer.h>

BEGIN_HA3_NAMESPACE(sql);

class KhronosDataScan : public KhronosScanBase {
protected:
    struct TagkColumnPack {
        TagkColumnPack() {}
        TagkColumnPack(ColumnData<autil::MultiChar> *columnDataIn)
            :columnData(columnDataIn)
        {}

        ColumnData<autil::MultiChar> *columnData;
        autil::MultiChar columnValueCache;
    };
    typedef std::map<std::string, TagkColumnPack> TagkColumnPackMap;
public:
    KhronosDataScan(const KhronosScanHints &scanHints, KhronosScanAccessLog *accessLog);
    virtual ~KhronosDataScan();
    KhronosDataScan(const KhronosDataScan&) = delete;
    KhronosDataScan& operator=(const KhronosDataScan &) = delete;
public:
    bool init(const ScanInitParam &param) override;
    bool doBatchScan(TablePtr &table, bool &eof) override;
private:
    virtual bool doBatchScanImpl(TablePtr &table, bool &eof) = 0;
    virtual bool parseQueryParamsFromCondition() = 0;
    virtual bool extractUsedField(OutputFieldsVisitor &visitor) = 0;
    bool parseUsedFields();
    std::string lookupParamsAsString();
    void reportInitFinishMetrics(bool success);
    void reportScanFinishMetrics(bool success);
protected:
    virtual bool initImpl(const ScanInitParam &param);
    bool updateTagkColumnValueCache(const std::string &seriesKey,
                                    TagkColumnPackMap &tagkColumnPackMap,
                                    bool skipWithMetricLength = false);
protected:
    KhronosDataScanInfo _khronosDataScanInfo;
    std::shared_ptr<autil::mem_pool::Pool> _kernelPool;
    autil::ConstString _metric;
    // used fields
    std::string _valueColName;
    // condition:
    // where
    khronos::search::SearchInterface::TagKVInfos _tagKVInfos;
    khronos::search::TimeRange _tsRange;
    CalcTablePtr _calcTable;
    khronos::search::DataIteratorPtr _rowIter;
    KHR_WM_TYPE _watermark;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(KhronosDataScan);

END_HA3_NAMESPACE(sql);
