#include <ha3/sql/ops/khronosScan/KhronosDataScan.h>
#include <ha3/sql/ops/khronosScan/KhronosUtil.h>
#include <ha3/sql/ops/condition/ConditionParser.h>
#include <ha3/sql/ops/condition/ExprUtil.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/sql/data/TableUtil.h>
#include <autil/legacy/RapidJsonHelper.h>

using namespace std;
using namespace autil;
using namespace autil_rapidjson;
using namespace khronos::search;
using namespace kmonitor;
IE_NAMESPACE_USE(index);

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, KhronosDataScan);

struct DataScanInitInfo {
    size_t rtScanLookup;
    size_t dataInitQueryPoolSize = 0;
    size_t dataInitKernelPoolSize = 0;
};

class DataScanInitMetrics : public MetricsGroup
{
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_QPS_MUTABLE_METRIC(_cntDataStageInitQps, "cntDataStageInitQps");
        REGISTER_LATENCY_MUTABLE_METRIC(_rtScanLookup, "rtScanLookup");
        REGISTER_GAUGE_MUTABLE_METRIC(_dataInitQueryPoolSize, "dataInitQueryPoolSize");
        REGISTER_GAUGE_MUTABLE_METRIC(_dataInitKernelPoolSize, "dataInitKernelPoolSize");
        REGISTER_QPS_MUTABLE_METRIC(_dataInitQueryPoolSizeAsQps, "dataInitQueryPoolSizeAsQps");
        REGISTER_QPS_MUTABLE_METRIC(_dataInitKernelPoolSizeAsQps, "dataInitKernelPoolSizeAsQps");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags, DataScanInitInfo *scanInitInfo) {
        REPORT_MUTABLE_QPS(_cntDataStageInitQps);
        REPORT_MUTABLE_METRIC(_rtScanLookup, scanInitInfo->rtScanLookup);
        if (scanInitInfo->dataInitQueryPoolSize > 0) {
            REPORT_MUTABLE_METRIC(_dataInitQueryPoolSize, scanInitInfo->dataInitQueryPoolSize);
            REPORT_MUTABLE_METRIC(_dataInitQueryPoolSizeAsQps, scanInitInfo->dataInitQueryPoolSize);
        }
        if (scanInitInfo->dataInitKernelPoolSize > 0) {
            REPORT_MUTABLE_METRIC(_dataInitKernelPoolSize, scanInitInfo->dataInitKernelPoolSize);
            REPORT_MUTABLE_METRIC(_dataInitKernelPoolSizeAsQps, scanInitInfo->dataInitKernelPoolSize);
        }
    }
private:
    MutableMetric *_cntDataStageInitQps = nullptr;
    MutableMetric *_rtScanLookup = nullptr;
    MutableMetric *_dataInitQueryPoolSize = nullptr;
    MutableMetric *_dataInitKernelPoolSize = nullptr;
    MutableMetric *_dataInitQueryPoolSizeAsQps = nullptr;
    MutableMetric *_dataInitKernelPoolSizeAsQps = nullptr;
};

struct DataScanFinishInfo {
    size_t cntCallNext;
    size_t cntPointScan;
    size_t cntSeriesOutput;
    size_t dataQueryPoolSize;
    size_t dataKernelPoolSize;
};

class DataScanFinishMetrics : public MetricsGroup
{
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_QPS_MUTABLE_METRIC(_cntDataStageScanQps, "cntDataStageScanQps");
        REGISTER_GAUGE_MUTABLE_METRIC(_cntCallNext, "cntCallNext");
        REGISTER_GAUGE_MUTABLE_METRIC(_cntPointScan, "cntPointScan");
        REGISTER_GAUGE_MUTABLE_METRIC(_cntSeriesOutput, "cntSeriesOutput");
        REGISTER_GAUGE_MUTABLE_METRIC(_dataQueryPoolSize, "dataQueryPoolSize");
        REGISTER_GAUGE_MUTABLE_METRIC(_dataKernelPoolSize, "dataKernelPoolSize");

        REGISTER_QPS_MUTABLE_METRIC(_cntCallNextAsQps, "cntCallNextAsQps");
        REGISTER_QPS_MUTABLE_METRIC(_cntPointScanAsQps, "cntPointScanAsQps");
        REGISTER_QPS_MUTABLE_METRIC(_cntSeriesOutputAsQps, "cntSeriesOutputAsQps");
        REGISTER_QPS_MUTABLE_METRIC(_dataQueryPoolSizeAsQps, "dataQueryPoolSizeAsQps");
        REGISTER_QPS_MUTABLE_METRIC(_dataKernelPoolSizeAsQps, "dataKernelPoolSizeAsQps");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags, DataScanFinishInfo *scanFinishInfo) {
        REPORT_MUTABLE_QPS(_cntDataStageScanQps);
        REPORT_MUTABLE_METRIC(_cntCallNext, scanFinishInfo->cntCallNext);
        REPORT_MUTABLE_METRIC(_cntPointScan, scanFinishInfo->cntPointScan);
        REPORT_MUTABLE_METRIC(_cntSeriesOutput, scanFinishInfo->cntSeriesOutput);
        REPORT_MUTABLE_METRIC(_dataQueryPoolSize, scanFinishInfo->dataQueryPoolSize);
        REPORT_MUTABLE_METRIC(_dataKernelPoolSize, scanFinishInfo->dataKernelPoolSize);

        REPORT_MUTABLE_METRIC(_cntCallNextAsQps, scanFinishInfo->cntCallNext);
        REPORT_MUTABLE_METRIC(_cntPointScanAsQps, scanFinishInfo->cntPointScan);
        REPORT_MUTABLE_METRIC(_cntSeriesOutputAsQps, scanFinishInfo->cntSeriesOutput);
        REPORT_MUTABLE_METRIC(_dataQueryPoolSizeAsQps, scanFinishInfo->dataQueryPoolSize);
        REPORT_MUTABLE_METRIC(_dataKernelPoolSizeAsQps, scanFinishInfo->dataKernelPoolSize);
    }
private:
    MutableMetric *_cntDataStageScanQps = nullptr;
    MutableMetric *_cntCallNext = nullptr;
    MutableMetric *_cntPointScan = nullptr;
    MutableMetric *_cntSeriesOutput = nullptr;
    MutableMetric *_dataQueryPoolSize = nullptr;
    MutableMetric *_dataKernelPoolSize = nullptr;
    MutableMetric *_cntCallNextAsQps = nullptr;
    MutableMetric *_cntPointScanAsQps = nullptr;
    MutableMetric *_cntSeriesOutputAsQps = nullptr;
    MutableMetric *_dataQueryPoolSizeAsQps = nullptr;
    MutableMetric *_dataKernelPoolSizeAsQps = nullptr;
};


KhronosDataScan::KhronosDataScan(const KhronosScanHints &scanHints, KhronosScanAccessLog *accessLog)
    : KhronosScanBase(scanHints, accessLog)
{
}

KhronosDataScan::~KhronosDataScan() {
}

bool KhronosDataScan::init(const ScanInitParam &param) {
    uint64_t initBegin = TimeUtility::currentTime();

    bool ret = initImpl(param);

    uint64_t initEnd = TimeUtility::currentTime();
    incInitTime(initEnd - initBegin);

    if (_accessLog) {
        if (ret) {
            _accessLog->setStatusCode(KhronosScanAccessLog::KAL_STATUS_INIT_FINISHED);
        } else {
            _accessLog->setStatusCode(KhronosScanAccessLog::KAL_STATUS_INIT_FAILED);
        }
        if (_timeoutTerminator) {
            auto quotaTime =
                _timeoutTerminator->getExpireTime() - _timeoutTerminator->getStartTime();
            _accessLog->setTimeoutQuota(quotaTime);
        }
        _accessLog->setInitTime(initEnd - initBegin);
        _accessLog->setKernelPoolSize(_kernelPool ? _kernelPool->getAllocatedSize() : 0);
        _accessLog->setQueryPoolSize(_pool ? _pool->getAllocatedSize() : 0);
    }

#define LOG_ON_FINISH(level)                                            \
    {                                                                   \
        SQL_LOG(level, "[init] khronos data scan info: [%s]",           \
                _khronosDataScanInfo.ShortDebugString().c_str());       \
        if (_pool) {                                                    \
            SQL_LOG(level,                                              \
                    "[init] query pool snapshot: "                      \
                    "alloc=[%lu] bytes, used=[%lu] bytes, total=[%lu] bytes", \
                    _pool->getAllocatedSize(), _pool->getUsedBytes(),   \
                    _pool->getTotalBytes());                            \
        }                                                               \
        if (_kernelPool) {                                              \
            SQL_LOG(level,                                              \
                    "[init] kernel pool snapshot: "                     \
                    "alloc=[%lu] bytes, used=[%lu] bytes, total=[%lu] bytes", \
                    _kernelPool->getAllocatedSize(), _kernelPool->getUsedBytes(), \
                    _kernelPool->getTotalBytes());                      \
        }                                                               \
    }
    if (ret) {
        reportInitFinishMetrics(true);
        LOG_ON_FINISH(DEBUG);
    } else {
        if (_rowIter) {
            reportSearchMetrics(false, _rowIter->GetSearchMetric());
        }
        reportInitFinishMetrics(false);
        SQL_LOG(INFO, "data scan Lookup params: %s", lookupParamsAsString().c_str());
        LOG_ON_FINISH(INFO);
    }
#undef LOG_ON_FINISH
    return ret;
}

bool KhronosDataScan::initImpl(const ScanInitParam &param) {
    if (!KhronosScanBase::init(param)) {
        SQL_LOG(ERROR, "init khronos scan base failed");
        return false;
    }
    _scanInfo.set_kernelname(param.opName);
    _scanInfo.set_nodename(param.nodeName);

    _kernelPool = _memoryPoolResource->getPool();
    _metric = ConstString(_tableName.c_str(), _kernelPool.get());
    _khronosDataScanInfo.set_metricname(_metric.toString());
    SQL_LOG(TRACE2, "data scan metric=[%s]", _metric.toString().c_str());

    if (!parseQueryParamsFromCondition()) {
        SQL_LOG(ERROR, "parse query params from condition failed");
        return false;
    }

    if (!parseUsedFields()) {
        SQL_LOG(ERROR, "parse used fields failed");
        return false;
    }

    // init calc
    SqlBizResource* bizResource = param.bizResource;
    if (!bizResource) {
        SQL_LOG(ERROR, "get sql biz resource failed.");
        return false;
    }
    auto stripedOutputFields = _outputFields;
    KernelUtil::stripName(stripedOutputFields);
    _calcTable.reset(new CalcTable(_pool, _memoryPoolResource,
                                   stripedOutputFields, _outputFieldsType,
                                   param.queryResource->getTracer(),
                                   param.queryResource->getSuezCavaAllocator(),
                                   bizResource->getCavaPluginManager(),
                                   bizResource->getFunctionInterfaceCreator().get()));
    if (!_calcTable->init(_outputExprsJson, _conditionJson)) {
        SQL_LOG(ERROR, "calc table init failed");
        return false;
    }

    assert(_tableReader);
    string stripedValueColName = _valueColName;
    KernelUtil::stripName(stripedValueColName);
    int ret;
    int64_t rt = 0;
    int64_t leftTime = _timeoutTerminator ? _timeoutTerminator->getLeftTime() :
        numeric_limits<int64_t>::max();
    {
        RTMetricTimer timer(rt);
        SQL_LOG(DEBUG, "data scan Lookup params: %s", lookupParamsAsString().c_str());
        if (_accessLog) {
            _accessLog->setQueryString(lookupParamsAsString());
        }
        ret = _tableReader->Lookup(_metric, _tsRange, _tagKVInfos, stripedValueColName,
                _kernelPool.get(), leftTime, _rowIter, _watermark);
    }
    _khronosDataScanInfo.set_lookuptime(rt);
    reportSimpleQps("tableReaderLookupQps", {{{"ret", to_string(ret)}}});
    if (_timeoutTerminator && _timeoutTerminator->getLeftTime() < 0) {
        ON_KHRONOS_SCAN_TERMINATOR_TIMEOUT("khronos data scan init timeout");
        reportSimpleQps("dataScanTimeoutQps");
        return false;
    }
    if (ret == SearchInterface::LEC_OK) {
        // pass
    } else if (ret == SearchInterface::LEC_LACK_INTEGRITY) {
        // pass
    } else {
        SQL_LOG(ERROR, "table reader lookup failed, ret=%d", ret);
        return false;
    }

    return true;
}

bool KhronosDataScan::doBatchScan(TablePtr &table, bool &eof) {
    eof = false;
    bool ret = doBatchScanImpl(table, eof);

    if (_accessLog) { // update each batch
        _accessLog->setScanTime(_scanInfo.totalseektime() + _scanInfo.totalevaluatetime());
        _accessLog->setKernelPoolSize(_kernelPool->getAllocatedSize());
        _accessLog->setQueryPoolSize(_pool->getAllocatedSize());
    }
    if (unlikely(!ret || eof)) {
        if (_accessLog) {
            if (!ret) {
                _accessLog->setStatusCode(KhronosScanAccessLog::KAL_STATUS_SCAN_FAILED);
            } else {
                _accessLog->setStatusCode(KhronosScanAccessLog::KAL_STATUS_ALL_FINISHED);
            }
        }

#define LOG_ON_FINISH(level)                                            \
        {                                                               \
            SQL_LOG(level, "[scan] khronos data scan search metric: [%s]", \
                    _rowIter->GetSearchMetric().toDebugString().c_str()); \
            SQL_LOG(level, "[scan] khronos data scan info: [%s]",       \
                    _khronosDataScanInfo.ShortDebugString().c_str());   \
            SQL_LOG(level,                                              \
                    "[scan] query pool snapshot: "                      \
                    "alloc=[%lu] bytes, used=[%lu] bytes, total=[%lu] bytes", \
                    _pool->getAllocatedSize(), _pool->getUsedBytes(),   \
                    _pool->getTotalBytes());                            \
            SQL_LOG(level,                                              \
                    "[scan] kernel pool snapshot: "                     \
                    "alloc=[%lu] bytes, used=[%lu] bytes, total=[%lu] bytes", \
                    _kernelPool->getAllocatedSize(), _kernelPool->getUsedBytes(), \
                    _kernelPool->getTotalBytes());                      \
        }
        if (ret) {
            reportSearchMetrics(true, _rowIter->GetSearchMetric());
            reportScanFinishMetrics(true);
            LOG_ON_FINISH(DEBUG);
        } else {
            reportSearchMetrics(false, _rowIter->GetSearchMetric());
            reportScanFinishMetrics(false);
            SQL_LOG(INFO, "data scan Lookup params: %s", lookupParamsAsString().c_str());
            LOG_ON_FINISH(INFO);
        }
#undef LOG_ON_FINISH
    }
    return ret;
}

bool KhronosDataScan::parseUsedFields() {
    AutilPoolAllocator allocator(_kernelPool.get());
    SimpleDocument outputExprsDoc(&allocator);
    outputExprsDoc.Parse(_outputExprsJson.c_str());
    if (outputExprsDoc.HasParseError()) {
        SQL_LOG(ERROR, "parse output exprs error, jsonStr [%s]", _outputExprsJson.c_str());
        return false;
    }
    if (!outputExprsDoc.IsObject()) {
        SQL_LOG(ERROR, "parsed result is not object, jsonStr [%s]", _outputExprsJson.c_str());
        return false;
    }

    string outputFieldsJson = autil::legacy::ToJsonString(_outputFields);
    SimpleDocument outputFieldsDoc(&allocator);
    outputFieldsDoc.Parse(outputFieldsJson.c_str());
    assert(!outputFieldsDoc.HasParseError());
    assert(outputFieldsDoc.IsArray());

    OutputFieldsVisitor visitor;
    for (size_t i = 0; i < outputFieldsDoc.Size(); ++i) {
        assert(outputFieldsDoc[i].IsString());
        string fieldName = outputFieldsDoc[i].GetString();
        if (outputExprsDoc.HasMember(fieldName)) {
            visitor.visit(outputExprsDoc[fieldName]);
        } else {
            visitor.visit(outputFieldsDoc[i]);
        }
        if (visitor.isError()) {
            SQL_LOG(ERROR, "visitor visit failed: %s", visitor.errorInfo().c_str());
            return false;
        }
    }

    if (!extractUsedField(visitor)) {
        SQL_LOG(ERROR, "extract used field failed");
        return false;
    }

    return true;
}

bool KhronosDataScan::updateTagkColumnValueCache(
        const string &seriesKey,
        TagkColumnPackMap &tagkColumnPackMap,
        bool skipWithMetricLength)
{
    char* emptyBuffer = autil::MultiValueCreator::createMultiValueBuffer("", 0, _pool);
    for (auto &pair : tagkColumnPackMap) {
        pair.second.columnValueCache.init(emptyBuffer);
    }

    const char *tagsStr = seriesKey.data();
    size_t len = seriesKey.size();
    size_t curPos = skipWithMetricLength ? _metric.size() : 0;

    // for performance, no more series key format check
    for (; curPos < len && tagsStr[curPos] != KHR_SERIES_KEY_METRIC_SPLITER; ++curPos);
    ++curPos;

    while (curPos < len)
    {
        size_t tagkBegin = curPos;
        // for performance, no more series key format check
        for (; curPos < len && tagsStr[curPos] != KHR_TAGKV_SPLITER; ++curPos);
        size_t tagkEnd = curPos;
        assert(tagkBegin < len);
        assert(tagkEnd <= len && tagkEnd >= tagkBegin);
        ++curPos;
        ConstString tagk{tagsStr + tagkBegin, tagkEnd - tagkBegin};

        if (unlikely(curPos > len)) { // curPos == len is valid
            SQL_LOG(ERROR, "tagv not found for tagk=[%s], skLen=[%lu], curPos=[%lu], sk=[%s]",
                    tagk.toString().c_str(), len, curPos, seriesKey.c_str());
            return false;
        }

        size_t tagvBegin = curPos;
        // for performance, no more series key format check
        for (; curPos < len && tagsStr[curPos] != KHR_SERIES_KEY_TAGS_SPLITER; ++curPos);
        size_t tagvEnd = curPos;
        assert(tagvEnd <= len && tagvEnd >= tagvBegin);
        ++curPos;

        ConstString tagv{tagsStr + tagvBegin, tagvEnd - tagvBegin};
        auto iter = tagkColumnPackMap.find(tagk.toString());
        if (iter != tagkColumnPackMap.end()) {
            char *buffer = MultiValueCreator::createMultiValueBuffer(
                    tagv.data(), tagv.size(), _pool);
            iter->second.columnValueCache.init(buffer);
        }
    }

    return true;
}

string KhronosDataScan::lookupParamsAsString() {
    ostringstream oss;
    oss << "catalog=[" << _catalogName << "], ";
    oss << "metric=[" << _metric.toString() << "], ";
    oss << "tsRange=[" << _tsRange.begin << ", " << _tsRange.end << "), ";
    oss << "tagKVInfos={" << StringUtil::toString(_tagKVInfos) << "}, ";
    oss << "valueName=[" << _valueColName << "]";
    return oss.str();
}

void KhronosDataScan::reportInitFinishMetrics(bool success) {
    DataScanInitInfo scanInitInfo;
    scanInitInfo.rtScanLookup = _khronosDataScanInfo.lookuptime();
    scanInitInfo.dataInitQueryPoolSize = _pool ?  _pool->getAllocatedSize() : 0;
    scanInitInfo.dataInitKernelPoolSize = _kernelPool ? _kernelPool->getAllocatedSize() : 0;
    reportUserMetrics<DataScanInitMetrics>({{{"status", success ? "success" : "failed"}}}, &scanInitInfo);
}

void KhronosDataScan::reportScanFinishMetrics(bool success) {
    DataScanFinishInfo scanFinishInfo;
    scanFinishInfo.cntCallNext = _khronosDataScanInfo.callnextcount();
    scanFinishInfo.cntPointScan = _khronosDataScanInfo.pointscancount();
    scanFinishInfo.cntSeriesOutput = _khronosDataScanInfo.seriesoutputcount();
    scanFinishInfo.dataQueryPoolSize = _pool->getAllocatedSize();
    scanFinishInfo.dataKernelPoolSize = _kernelPool->getAllocatedSize();
    reportUserMetrics<DataScanFinishMetrics>({{{"status", success ? "success" : "failed"}}}, &scanFinishInfo);
}


END_HA3_NAMESPACE(sql);
