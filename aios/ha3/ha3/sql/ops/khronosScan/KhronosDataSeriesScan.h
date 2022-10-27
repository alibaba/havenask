#pragma once

#include <ha3/sql/ops/khronosScan/KhronosDataScan.h>
#include <ha3/sql/ops/khronosScan/KhronosCommon.h>
#include <khronos_table_interface/CommonDefine.h>
#include <khronos_table_interface/LineSegmentBuffer.h>
#include <khronos_table_interface/DataSeries.hpp>

BEGIN_HA3_NAMESPACE(sql);

class KhronosDataSeriesScan : public KhronosDataScan {
public:
    KhronosDataSeriesScan(const KhronosScanHints &scanHints = {},
                          KhronosScanAccessLog *accessLog = nullptr);
    virtual ~KhronosDataSeriesScan();
    KhronosDataSeriesScan(const KhronosDataSeriesScan&) = delete;
    KhronosDataSeriesScan& operator=(const KhronosDataSeriesScan &) = delete;
public:
    bool doBatchScanImpl(TablePtr &table, bool &eof) override;
private:
    bool parseQueryParamsFromCondition() override;
    bool extractUsedField(OutputFieldsVisitor &visitor) override;
    bool outputRow(TablePtr &table,
                   ColumnData<autil::MultiChar> *valueColumnData,
                   ColumnData<KHR_WM_TYPE> *watermarkColumnData,
                   TagkColumnPackMap &tagkColumnPackMap,
                   int64_t &batchSize);
    bool declareTableColumn(TablePtr &table,
                            ColumnData<autil::MultiChar> *&valueColumnData,
                            ColumnData<KHR_WM_TYPE> *&watermarkColumnData,
                            TagkColumnPackMap &tagkColumnPackMap);
protected:
    bool initImpl(const ScanInitParam &param) override;
private:
    size_t _dataPoolBytesLimit;
    // used fields:
    // select tags['host'], tags['mem']
    std::set<std::string> _usedFieldsTagkSet;
    std::string _watermarkColName;
    khronos::LineSegmentBufferPtr _lineSegmentBuffer;
    khronos::DataSeriesWriteOnly _dataSeriesWrite;
    uint64_t _lastHash;
    std::string _lastSk;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(KhronosDataSeriesScan);

END_HA3_NAMESPACE(sql);
