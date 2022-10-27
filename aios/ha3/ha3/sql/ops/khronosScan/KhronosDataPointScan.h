#pragma once

#include <ha3/sql/ops/khronosScan/KhronosDataScan.h>
#include <ha3/sql/ops/khronosScan/KhronosCommon.h>
#include <khronos_table_interface/CommonDefine.h>
#include <khronos_table_interface/LineSegmentBuffer.h>

BEGIN_HA3_NAMESPACE(sql);

class KhronosDataPointScan : public KhronosDataScan {
public:
    KhronosDataPointScan(const KhronosScanHints &scanHints = {},
                         KhronosScanAccessLog *accessLog = nullptr);
    virtual ~KhronosDataPointScan();
    KhronosDataPointScan(const KhronosDataPointScan&) = delete;
    KhronosDataPointScan& operator=(const KhronosDataPointScan &) = delete;
public:
    bool doBatchScanImpl(TablePtr &table, bool &eof) override;
private:
    bool parseQueryParamsFromCondition() override;
    bool extractUsedField(OutputFieldsVisitor &visitor) override;
    bool declareTableColumn(TablePtr &table,
                            ColumnData<KHR_TS_TYPE> *&tsColumnData,
                            ColumnData<KHR_WM_TYPE> *&watermarkColumnData,
                            ColumnData<KHR_VALUE_TYPE> *&valueColumnData,
                            TagkColumnPackMap &tagkColumnPackMap);
    size_t getBatchSize(TablePtr &table);
protected:
    bool initImpl(const ScanInitParam &param) override;
private:
    // used fields:
    // select tags['host'], tags['mem']
    std::set<std::string> _usedFieldsTagkSet;
    std::string _tsColName;
    std::string _watermarkColName;
    khronos::LineSegmentBufferPtr _lineSegmentBuffer;
    size_t _bufOffset;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(KhronosDataPointScan);

END_HA3_NAMESPACE(sql);
