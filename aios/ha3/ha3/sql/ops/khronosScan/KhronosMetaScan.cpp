#include <ha3/sql/ops/khronosScan/KhronosMetaScan.h>
#include <ha3/sql/ops/khronosScan/KhronosCommon.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/sql/resource/SqlQueryResource.h>

using namespace std;
using namespace autil;
using namespace khronos::search;
using namespace kmonitor;
IE_NAMESPACE_USE(index);

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, KhronosMetaScan);

struct MetaScanInitInfo {
    size_t metaInitQueryPoolSize;
    size_t metaInitKernelPoolSize;
};

class MetaScanInitMetrics : public MetricsGroup
{
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_QPS_MUTABLE_METRIC(_cntMetaStageInitQps, "cntMetaStageInitQps");
        REGISTER_GAUGE_MUTABLE_METRIC(_metaInitQueryPoolSize, "metaInitQueryPoolSize");
        REGISTER_GAUGE_MUTABLE_METRIC(_metaInitKernelPoolSize, "metaInitKernelPoolSize");
        REGISTER_QPS_MUTABLE_METRIC(_metaInitQueryPoolSizeAsQps, "metaInitQueryPoolSizeAsQps");
        REGISTER_QPS_MUTABLE_METRIC(_metaInitKernelPoolSizeAsQps, "metaInitKernelPoolSizeAsQps");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags, MetaScanInitInfo *scanInitInfo) {
        REPORT_MUTABLE_QPS(_cntMetaStageInitQps);
        if (scanInitInfo->metaInitQueryPoolSize > 0) {
            REPORT_MUTABLE_METRIC(_metaInitQueryPoolSize, scanInitInfo->metaInitQueryPoolSize);
            REPORT_MUTABLE_METRIC(_metaInitQueryPoolSizeAsQps, scanInitInfo->metaInitQueryPoolSize);
        }
        if (scanInitInfo->metaInitKernelPoolSize > 0) {
            REPORT_MUTABLE_METRIC(_metaInitKernelPoolSize, scanInitInfo->metaInitKernelPoolSize);
            REPORT_MUTABLE_METRIC(_metaInitKernelPoolSizeAsQps, scanInitInfo->metaInitKernelPoolSize);
        }
    }
private:
    MutableMetric *_cntMetaStageInitQps = nullptr;
    MutableMetric *_metaInitQueryPoolSize = nullptr;
    MutableMetric *_metaInitKernelPoolSize = nullptr;
    MutableMetric *_metaInitQueryPoolSizeAsQps = nullptr;
    MutableMetric *_metaInitKernelPoolSizeAsQps = nullptr;
};

struct MetaScanFinishInfo {
    size_t rtMetaLookup;
    size_t metaQueryPoolSize;
    size_t metaKernelPoolSize;
};

class MetaScanFinishMetrics : public MetricsGroup
{
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_QPS_MUTABLE_METRIC(_cntMetaStageScanQps, "cntMetaStageScanQps");
        REGISTER_LATENCY_MUTABLE_METRIC(_rtMetaLookup, "rtMetaLookup");
        REGISTER_GAUGE_MUTABLE_METRIC(_metaQueryPoolSize, "metaQueryPoolSize");
        REGISTER_GAUGE_MUTABLE_METRIC(_metaKernelPoolSize, "metaKernelPoolSize");
        REGISTER_QPS_MUTABLE_METRIC(_metaQueryPoolSizeAsQps, "metaQueryPoolSizeAsQps");
        REGISTER_QPS_MUTABLE_METRIC(_metaKernelPoolSizeAsQps, "metaKernelPoolSizeAsQps");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags, MetaScanFinishInfo *scanFinishInfo) {
        REPORT_MUTABLE_QPS(_cntMetaStageScanQps);
        REPORT_MUTABLE_METRIC(_rtMetaLookup, scanFinishInfo->rtMetaLookup);
        REPORT_MUTABLE_METRIC(_metaQueryPoolSize, scanFinishInfo->metaQueryPoolSize);
        REPORT_MUTABLE_METRIC(_metaKernelPoolSize, scanFinishInfo->metaKernelPoolSize);
        REPORT_MUTABLE_METRIC(_metaQueryPoolSizeAsQps, scanFinishInfo->metaQueryPoolSize);
        REPORT_MUTABLE_METRIC(_metaKernelPoolSizeAsQps, scanFinishInfo->metaKernelPoolSize);
    }
private:
    MutableMetric *_cntMetaStageScanQps = nullptr;
    MutableMetric *_rtMetaLookup = nullptr;
    MutableMetric *_metaQueryPoolSize = nullptr;
    MutableMetric *_metaKernelPoolSize = nullptr;
    MutableMetric *_metaQueryPoolSizeAsQps = nullptr;
    MutableMetric *_metaKernelPoolSizeAsQps = nullptr;
};

KhronosMetaScan::KhronosMetaScan(const KhronosScanHints &scanHints,
                                 KhronosScanAccessLog *accessLog)
    : KhronosScanBase(scanHints, accessLog)
{
}

KhronosMetaScan::~KhronosMetaScan() {
}

bool KhronosMetaScan::init(const ScanInitParam &param) {
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
        _accessLog->setInitTime(initEnd - initBegin);
        _accessLog->setQueryPoolSize(_pool ? _pool->getAllocatedSize() : 0);
    }

#define LOG_ON_FINISH(level)                                            \
    {                                                                   \
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
        reportInitFinishMetrics(false);
        SQL_LOG(INFO, "meta scan Lookup params: %s", lookupParamsAsString().c_str());
        LOG_ON_FINISH(INFO);
    }
#undef LOG_ON_FINISH
    return ret;
}

bool KhronosMetaScan::initImpl(const ScanInitParam &param) {
    if (!KhronosScanBase::init(param)) {
        SQL_LOG(ERROR, "init khronos scan base failed");
        return false;
    }
    _kernelPool = _memoryPoolResource->getPool();
    if (_outputFields.size() != 1) {
        SQL_LOG(ERROR, "output fields size is [%lu], must be 1", _outputFields.size());
        return false;
    }
    if (!fillScanType(_outputFields[0])) {
        SQL_LOG(ERROR, "fill scan type failed");
        return false;
    }

    if (!parseQueryParamsFromCondition()) {
        SQL_LOG(ERROR, "parse query params from condition failed");
        return false;
    }

    if (_accessLog) {
        _accessLog->setQueryString(lookupParamsAsString());
    }

    return true;
}

bool KhronosMetaScan::fillScanType(const std::string &columnName) {
    auto iter = _indexInfos.find(columnName);
    if (iter == _indexInfos.end()) {
        SQL_LOG(ERROR, "field [%s] index info not found", columnName.c_str());
        return false;
    }
    auto &indexInfo = iter->second;
    auto &indexType = indexInfo.type;
    if (indexType == KHRONOS_METRIC_TYPE) {
        _scanType = KST_METRIC;
    } else if (indexType == KHRONOS_TAG_KEY_TYPE) {
        _scanType = KST_TAGK;
    } else if (indexType == KHRONOS_TAG_VALUE_TYPE) {
        _scanType = KST_TAGV;
    } else {
        SQL_LOG(ERROR, "index type [%s] is not supported", indexType.c_str());
        return false;
    }

    SQL_LOG(TRACE2, "meta scan type: %d", _scanType);
    return true;
}

bool KhronosMetaScan::parseQueryParamsFromCondition()
{
    KhronosMetaConditionVisitor visitor(_indexInfos);
    if (!visitAndParseCondition(&visitor)) {
        SQL_LOG(ERROR, "visit and parse condition failed");
        return false;
    }

    auto &metricVec = visitor.getMetricVec();
    auto &tagkVec = visitor.getTagkVec();
    auto &tagvVec = visitor.getTagvVec();
    _tsRange = visitor.getTsRange();

#define CHECK_COND_MACRO(condition, hint)       \
    if (!(condition)) {                         \
        SQL_LOG(ERROR, "" hint);                \
        return false;                           \
    }

    CHECK_COND_MACRO(_tsRange.IsValid(), "time stamp range is invalid");

    switch (_scanType) {
    case KST_METRIC: {
        SQL_LOG(TRACE2, "handle params with mode [KST_METRIC]");
        if (metricVec.size() == 0) {
            _metric = {"", SearchInterface::METRIC_PREFIX};
        } else if (metricVec.size() == 1) {
            _metric = metricVec[0];
            CHECK_COND_MACRO(_metric.mMetricMatchType != SearchInterface::METRIC_PLAIN_TEXT,
                "SELECT METRIC: metric condition can not be exact match");
        } else {
            CHECK_COND_MACRO(false, "SELECT METRIC: metric condition num must less than 2");
        }
        CHECK_COND_MACRO(tagkVec.empty(), "SELECT METRIC: tagk condition must be empty");
        CHECK_COND_MACRO(tagvVec.empty(), "SELECT METRIC: tagv condition must be empty");
        break;
    }
    case KST_TAGK: {
        SQL_LOG(TRACE2, "handle params with mode [KST_TAGK]");
        if (metricVec.size() == 0) {
            _metric = {"", SearchInterface::METRIC_PLAIN_TEXT};
        } else if (metricVec.size() == 1) {
            _metric = metricVec[0];
            CHECK_COND_MACRO(_metric.mMetricMatchType == SearchInterface::METRIC_PLAIN_TEXT,
                  "SELECT TAGK: metric condition must be exact match");
        } else {
            CHECK_COND_MACRO(false, "SELECT TAGK: metric condition num must less than 2");
        }
        CHECK_COND_MACRO(tagkVec.empty(), "SELECT TAGK: tagk condition must be empty");
        CHECK_COND_MACRO(tagvVec.empty(), "SELECT TAGK: tagv condition must be empty");
        break;
    }
    case KST_TAGV: {
        SQL_LOG(TRACE2, "handle params with mode [KST_TAGV]");
        if (metricVec.size() == 0) {
            _metric = {"", SearchInterface::METRIC_PLAIN_TEXT};
        } else if (metricVec.size() == 1) {
            _metric = metricVec[0];
            CHECK_COND_MACRO(_metric.mMetricMatchType == SearchInterface::METRIC_PLAIN_TEXT,
                  "SELECT TAGV: metric condition must be exact match");
        } else {
            CHECK_COND_MACRO(false, "SELECT TAGV: metric condition num must less than 2");
        }
        CHECK_COND_MACRO(tagkVec.size() == 1, "SELECT TAGV: tagk condition num must be 1");
        CHECK_COND_MACRO(!StringUtil::endsWith(tagkVec[0], "*"),
              "SELECT TAGV: tagk condition can not end with *");
        _tagk = tagkVec[0];
        CHECK_COND_MACRO(tagvVec.size() < 2, "SELECT TAGV: tagv condition num must less than 2");
        if (tagvVec.size() == 1) {
            CHECK_COND_MACRO(StringUtil::endsWith(tagvVec[0], "*"),
                  "SELECT TAGV: tagv condition must end with *");
            _tagv = tagvVec[0];
            _tagv.pop_back();
        }
        break;
    }
    default:
        assert(false);
    }
#undef CHECK_COND_MACRO

    SQL_LOG(DEBUG, "meta scan condition: metric=[%s], "
            "tagk=[%s], tagv=[%s], tsRange=[%ld, %ld), limit=[%d]",
            _metric.toString().c_str(), _tagk.c_str(), _tagv.c_str(),
            _tsRange.begin, _tsRange.end, _limit);
    return true;
}

bool KhronosMetaScan::doBatchScan(TablePtr &table, bool &eof) {
    eof = false;

    bool ret = doBatchScanImpl(table, eof);

    if (unlikely(!ret || eof)) {
        if (_accessLog) {
            if (!ret) {
                _accessLog->setStatusCode(KhronosScanAccessLog::KAL_STATUS_SCAN_FAILED);
            } else {
                _accessLog->setStatusCode(KhronosScanAccessLog::KAL_STATUS_ALL_FINISHED);
            }
            _accessLog->setScanTime(_scanInfo.totalseektime() + _scanInfo.totalevaluatetime());
            _accessLog->setKernelPoolSize(_kernelPool->getAllocatedSize());
            _accessLog->setQueryPoolSize(_pool->getAllocatedSize());
        }
#define LOG_ON_FINISH(level)                                            \
        {                                                               \
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
            reportScanFinishMetrics(true);
            LOG_ON_FINISH(DEBUG);
        } else {
            reportScanFinishMetrics(false);
            SQL_LOG(INFO, "meta scan Lookup params: %s", lookupParamsAsString().c_str());
            LOG_ON_FINISH(INFO);
        }
#undef LOG_ON_FINISH
    }
    return ret;
}

bool KhronosMetaScan::doBatchScanImpl(TablePtr &table, bool &eof) {
    std::set<std::string> uniqueSet;
    int32_t errorCode = 0;
    assert(_tableReader);

    int64_t leftTime = _timeoutTerminator ? _timeoutTerminator->getLeftTime() :
        numeric_limits<int64_t>::max();

    uint32_t resultLimit = 0;
    switch (_scanType) {
    case KST_METRIC: {
        resultLimit = _limit;
        errorCode = _tableReader->LookupMetaMetric(_tsRange, _metric,
                                                   uniqueSet, resultLimit, leftTime);
        reportSimpleQps("tableReaderLookupMetaMetricQps", {{{"ret", to_string(errorCode)}}});
        break;
    }
    case KST_TAGK: {
        // Tagk scan ignore limit param
        resultLimit = std::numeric_limits<uint32_t>::max();
        errorCode = _tableReader->LookupMetaTagk(_tsRange, _metric.mMetricPattern,
                                                 uniqueSet, leftTime);
        reportSimpleQps("tableReaderLookupMetaTagkQps", {{{"ret", to_string(errorCode)}}});
        break;
    }
    case KST_TAGV: {
        resultLimit = _limit;
        errorCode = _tableReader->LookupMetaTagv(_tsRange, _tagk,
                                                 _tagv, _metric.mMetricPattern, uniqueSet,
                                                 resultLimit, leftTime);
        reportSimpleQps("tableReaderLookupMetaTagvQps", {{{"ret", to_string(errorCode)}}});
        break;
    }
    default:
        assert(false);
    }

    uint64_t afterSeek = TimeUtility::currentTime();
    incSeekTime(afterSeek - _batchScanBeginTime);

    if (_timeoutTerminator && _timeoutTerminator->getLeftTime() < 0) {
        ON_KHRONOS_SCAN_TERMINATOR_TIMEOUT("khronos meta scan lookup timeout");
        reportSimpleQps("metaScanTimeoutQps");
        return false;
    }

    if (errorCode != 0) {
        SearchMetric searchMetric;
        searchMetric.values[CNT_IO_EXCEPTION] = 1;
        reportSearchMetrics(false, searchMetric);
        SQL_LOG(ERROR, "exception caught in metaScan");
        return false;
    }
    std::string outputField = _outputFields[0];
    KernelUtil::stripName(outputField);
    if (!createSingleColumnTable(uniqueSet, outputField, resultLimit, table)) {
        SQL_LOG(ERROR, "create [%s] table failed", outputField.c_str());
        return false;
    }
    int64_t outputTime = TimeUtility::currentTime();
    incOutputTime(outputTime - afterSeek);
    _seekCount = table->getRowCount();
    eof = true;
    return true;
}

bool KhronosMetaScan::createSingleColumnTable(
        const std::set<std::string> &uniqueSet,
        const std::string &colName,
        size_t limit,
        TablePtr &table)
{
    table.reset(new Table(_kernelPool));
    ColumnPtr columnPtr = table->declareColumn<autil::MultiChar>(colName);
    if (!columnPtr) {
        HA3_LOG(ERROR, "table declare column failed");
        return false;
    }
    size_t rows = std::min(uniqueSet.size(), limit);
    table->batchAllocateRow(rows);
    SQL_LOG(TRACE2, "current batch %lu rows allocated", rows);

    auto *colData = columnPtr->getColumnData<autil::MultiChar>();
    size_t rowIdx = 0;
    autil::MultiChar mc;
    for (auto &item: uniqueSet) {
        if (rowIdx >= rows) {
            break;
        }
        char *buffer = autil::MultiValueCreator::createMultiValueBuffer<char>(
                item.data(), item.size(), _pool);
        mc.init(buffer);
        colData->set(rowIdx, mc);
        ++rowIdx;
    }
    return true;
}

string KhronosMetaScan::lookupParamsAsString() {
    ostringstream oss;
    oss << "catalog=[" << _catalogName << "], ";
    oss << "metric=[" << _metric.toString() << "], ";
    oss << "tagk=[" << _tagk << "], ";
    oss << "tagv=[" << _tagv << "], ";
    oss << "tsRange=[" << _tsRange.begin << ", " << _tsRange.end << "), ";
    oss << "limit=[" << _limit << "]";
    return oss.str();
}

void KhronosMetaScan::reportInitFinishMetrics(bool success) {
    MetaScanInitInfo scanInitInfo;
    scanInitInfo.metaInitQueryPoolSize = _pool ? _pool->getAllocatedSize() : 0;
    scanInitInfo.metaInitKernelPoolSize = _kernelPool ? _kernelPool->getAllocatedSize() : 0;
    reportUserMetrics<MetaScanInitMetrics>({{{"status", success ? "success" : "failed"}}}, &scanInitInfo);
}

void KhronosMetaScan::reportScanFinishMetrics(bool success) {
    MetaScanFinishInfo scanFinishInfo;
    scanFinishInfo.rtMetaLookup = _scanInfo.totalseektime();
    scanFinishInfo.metaQueryPoolSize = _pool->getAllocatedSize();
    scanFinishInfo.metaKernelPoolSize = _kernelPool->getAllocatedSize();
    reportUserMetrics<MetaScanFinishMetrics>({{{"status", success ? "success" : "failed"}}}, &scanFinishInfo);
}

END_HA3_NAMESPACE(sql);
