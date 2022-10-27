#include <ha3/sql/ops/khronosScan/KhronosDataSeriesScan.h>
#include <ha3/sql/ops/khronosScan/KhronosDataSeriesConditionVisitor.h>
#include <ha3/sql/ops/khronosScan/KhronosUtil.h>
#include <ha3/sql/ops/condition/ConditionParser.h>
#include <ha3/sql/ops/condition/ExprUtil.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/sql/data/TableUtil.h>
#include <autil/legacy/RapidJsonHelper.h>

using namespace std;
using namespace autil;
using namespace autil_rapidjson;
using namespace khronos;
using namespace khronos::search;
IE_NAMESPACE_USE(index);

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, KhronosDataSeriesScan);

KhronosDataSeriesScan::KhronosDataSeriesScan(const KhronosScanHints &scanHints,
                                             KhronosScanAccessLog *accessLog)
    : KhronosDataScan(scanHints, accessLog)
    , _dataPoolBytesLimit(KHR_SERIES_SCAN_DATA_POOL_LIMIT)
    , _lastHash(0)
{
}

KhronosDataSeriesScan::~KhronosDataSeriesScan() {
}

bool KhronosDataSeriesScan::initImpl(const ScanInitParam &param) {
    if (!KhronosDataScan::initImpl(param)) {
        SQL_LOG(ERROR, "init khronos data scan failed");
        return false;
    }
    POOL_NEW_CLASS_ON_SHARED_PTR(
            _kernelPool.get(), _lineSegmentBuffer,
            LineSegmentBuffer, KHR_DEFAULT_BUFFER_COUNT, 1, _kernelPool.get());
    return true;
}

bool KhronosDataSeriesScan::parseQueryParamsFromCondition() {
    KhronosDataSeriesConditionVisitor visitor(_kernelPool.get(), _indexInfos);
    if (!visitAndParseCondition(&visitor)) {
        SQL_LOG(ERROR, "visit and parse condition failed");
        return false;
    }
    // steal the query params
    using std::swap;
    swap(_tagKVInfos, visitor.getTagKVInfos());
    swap(_tsRange, visitor.getTsRange());

#define CHECK_COND_MACRO(condition, hint)       \
    if (!(condition)) {                         \
        SQL_LOG(ERROR, "" hint);                \
        return false;                           \
    }
    CHECK_COND_MACRO(_tsRange.IsValid(), "time stamp range is invalid");
#undef CHECK_COND_MACRO

    SQL_LOG(TRACE2, "data series scan condition: tsRange=[%ld, %ld) tagk={%s}",
            _tsRange.begin, _tsRange.end, StringUtil::toString(_tagKVInfos).c_str());

    return true;
}

bool KhronosDataSeriesScan::extractUsedField(OutputFieldsVisitor &visitor) {
    auto &usedFieldsColumnSet = visitor.getUsedFieldsColumnSet();
    for (auto &item : usedFieldsColumnSet) {
        if (KhronosUtil::typeMatch(_indexInfos, item, KHRONOS_DATA_SERIES_VALUE_TYPE)) {
            if (!_valueColName.empty()) {
                SQL_LOG(ERROR, "only one value field is supported: [%s] vs [%s]",
                        _valueColName.c_str(), item.c_str());
                return false;
            }
            _valueColName = item;
        } else if (KhronosUtil::typeMatch(_indexInfos, item, KHRONOS_WATERMARK_TYPE)) {
            if (!_watermarkColName.empty()) {
                SQL_LOG(ERROR, "only one watermark field is supported: [%s] vs [%s]",
                        _watermarkColName.c_str(), item.c_str());
                return false;
            }
            _watermarkColName = item;
        } else {
            SQL_LOG(ERROR, "field [%s] is not supported in khronos data series scan",
                    item.c_str());
            return false;
        }
    }

    if (_valueColName.empty()) {
        SQL_LOG(ERROR, "value column field must be used in query");
        return false;
    }

    auto &usedFieldsItemSet = visitor.getUsedFieldsItemSet();
    // TODO
    // tags indexinfo
    for (auto &item : usedFieldsItemSet) {
        if (item.first != KHRONOS_TAGS_PREFIX) {
            SQL_LOG(ERROR, "tags name mismatch: [%s]", item.first.c_str());
            return false;
        }
        _usedFieldsTagkSet.insert(item.second);
    }

    SQL_LOG(TRACE2, "data series scan used fields: value=[%s], watermark=[%s], tagk=[%s]",
            _valueColName.c_str(), _watermarkColName.c_str(),
            StringUtil::toString(_usedFieldsTagkSet.begin(), _usedFieldsTagkSet.end()).c_str());
    return true;
}

bool KhronosDataSeriesScan::doBatchScanImpl(TablePtr &table, bool &eof) {
    assert(sizeof(DataPoint) == LineSegmentPoint::GetMemorySize(1));
    assert(_lineSegmentBuffer != nullptr);
    assert(_rowIter != nullptr);

    table.reset(new Table(_kernelPool));

    ColumnData<autil::MultiChar> *valueColumnData = nullptr;
    ColumnData<KHR_WM_TYPE> *watermarkColumnData = nullptr;
    TagkColumnPackMap tagkColumnPackMap;
    if (!declareTableColumn(table, valueColumnData, watermarkColumnData, tagkColumnPackMap)) {
        SQL_LOG(ERROR, "declare table column failed");
        return false;
    }

    int64_t batchBytes = _batchSize > 0 ? _batchSize : DEFAULT_BATCH_SIZE;

    auto &lineSegmentBuffer = *_lineSegmentBuffer;
    int32_t errorCode = 0;
    size_t nums = 0u;

#define WRITE_LAST_SERIES()                                             \
    if (!_lastSk.empty()) {                                             \
        if (!outputRow(table, valueColumnData, watermarkColumnData,     \
                       tagkColumnPackMap, batchBytes)) {                \
            SQL_LOG(ERROR, "output row failed, sk [%s], sh [%lu]",      \
                    _lastSk.c_str(), _lastHash);                        \
            return false;                                               \
        }                                                               \
    }

    while (batchBytes > 0 && _limit > 0) {
        if (unlikely(_timeoutTerminator && _timeoutTerminator->checkTimeout())) {
            ON_KHRONOS_SCAN_TERMINATOR_TIMEOUT("run timeout");
            reportSimpleQps("dataScanTimeoutQps");
            return false;
        }

        nums = _rowIter->Next(lineSegmentBuffer, errorCode);
        if (unlikely(errorCode != 0)) {
            SQL_LOG(ERROR, "caught IO exception, errorMsg=[%s]", _rowIter->GetLastError().c_str());
            return false;
        }
        _khronosDataScanInfo.set_callnextcount(_khronosDataScanInfo.callnextcount() + 1);
        if (nums == 0) {
            WRITE_LAST_SERIES();
            eof = true;
            break;
        }
        _khronosDataScanInfo.set_pointscancount(
                _khronosDataScanInfo.pointscancount() + lineSegmentBuffer.size());
        uint64_t curHash = lineSegmentBuffer.getSeriesHash();
        if (curHash != _lastHash) {
            WRITE_LAST_SERIES();
            _lastHash = curHash;
            _lastSk = lineSegmentBuffer.getSeriesKey();
        }

        _dataSeriesWrite.batchAppend(
                (khronos::DataPoint *)lineSegmentBuffer.data(), lineSegmentBuffer.size());
        size_t linePointCount = _dataSeriesWrite.pointCount();
        if (unlikely(linePointCount > _scanHints.oneLineScanPointLimit)) {
            SQL_LOG(ERROR, "current line point scan count exceeded, limit=[%lu], actual=[%lu]",
                    _scanHints.oneLineScanPointLimit, linePointCount);
            return false;
        }
    }

    uint64_t afterSeek = TimeUtility::currentTime();
    incSeekTime(afterSeek - _batchScanBeginTime);
    if (!_calcTable->projectTable(table)) {
        SQL_LOG(ERROR, "project table failed");
        return false;
    }
    uint64_t afterEvaluate = TimeUtility::currentTime();
    incEvaluateTime(afterEvaluate - afterSeek);
    _seekCount = table->getRowCount();
    eof |= _limit == 0;
    return true;
}

bool KhronosDataSeriesScan::outputRow(TablePtr &table,
                                      ColumnData<autil::MultiChar> *valueColumnData,
                                      ColumnData<KHR_WM_TYPE> *watermarkColumnData,
                                      TagkColumnPackMap &tagkColumnPackMap,
                                      int64_t &batchBytes)
{
    // update value cache when sk changed
    if (!updateTagkColumnValueCache(_lastSk, tagkColumnPackMap, true)) {
        SQL_LOG(ERROR, "update tagk column value cache failed");
        return false;
    }
    Row tableRow = table->allocateRow();
    autil::MultiChar multiChar;
    if (!_dataSeriesWrite.serialize(_pool, multiChar)) {
        SQL_LOG(ERROR, "serialize data to MultiChar failed");
        return false;
    }
    assert(valueColumnData != nullptr);
    valueColumnData->set(tableRow, multiChar);

    if (watermarkColumnData != nullptr) {
        watermarkColumnData->set(tableRow, _watermark);
    }

    for (auto &pair : tagkColumnPackMap) {
        pair.second.columnData->set(tableRow, pair.second.columnValueCache);
    }
    _khronosDataScanInfo.set_seriesoutputcount(_khronosDataScanInfo.seriesoutputcount() + 1);

    assert(batchBytes > 0);
    assert(_limit > 0);
    batchBytes -= table->getRowSize() + multiChar.size() * KHR_COMPRESSION_BYTES_FACTOR;
    --_limit;

    _dataSeriesWrite.reset();

    if (unlikely(_pool->getAllocatedSize() > _dataPoolBytesLimit)) {
        SQL_LOG(ERROR, "pool used too many bytes, limit=[%lu], actual=[%lu]",
                _dataPoolBytesLimit, _pool->getAllocatedSize());
        return false;
    }

    return true;
}

bool KhronosDataSeriesScan::declareTableColumn(TablePtr &table,
        ColumnData<autil::MultiChar> *&valueColumnData,
        ColumnData<KHR_WM_TYPE> *&watermarkColumnData,
        TagkColumnPackMap &tagkColumnPackMap)
{
    {
        assert(!_valueColName.empty());
        string stripedValueColName = _valueColName;
        KernelUtil::stripName(stripedValueColName);
        valueColumnData = TableUtil::declareAndGetColumnData<autil::MultiChar>(
                table, stripedValueColName);
        if (valueColumnData == nullptr) {
            SQL_LOG(ERROR, "declare value column [%s] failed", stripedValueColName.c_str());
            return false;
        }
    }
    if (!_watermarkColName.empty()) {
        string stripedWatermarkColName = _watermarkColName;
        KernelUtil::stripName(stripedWatermarkColName);
        watermarkColumnData = TableUtil::declareAndGetColumnData<KHR_WM_TYPE>(
                table, stripedWatermarkColName);
    }
    for (auto &tagk : _usedFieldsTagkSet) {
        string colName = ExprUtil::getItemFullName(KHRONOS_TAGS_PREFIX, tagk);
        string stripedColName = colName;
        KernelUtil::stripName(stripedColName);
        auto *colData =
            TableUtil::declareAndGetColumnData<autil::MultiChar>(table, stripedColName);
        if (colData == nullptr) {
            SQL_LOG(ERROR, "declare tagk column [%s] failed", stripedColName.c_str());
            return false;
        }
        tagkColumnPackMap[tagk] = {colData};
    }
    table->endGroup();
    return true;
}

END_HA3_NAMESPACE(sql);
