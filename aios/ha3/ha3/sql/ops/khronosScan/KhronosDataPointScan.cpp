#include <ha3/sql/ops/khronosScan/KhronosDataPointScan.h>
#include <ha3/sql/ops/khronosScan/KhronosDataPointConditionVisitor.h>
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
HA3_LOG_SETUP(sql, KhronosDataPointScan);

KhronosDataPointScan::KhronosDataPointScan(const KhronosScanHints &scanHints,
        KhronosScanAccessLog *accessLog)
    : KhronosDataScan(scanHints, accessLog)
    , _bufOffset(0)
{
}

KhronosDataPointScan::~KhronosDataPointScan() {
}

bool KhronosDataPointScan::initImpl(const ScanInitParam &param) {
    if (!KhronosDataScan::initImpl(param)) {
        SQL_LOG(ERROR, "init khronos data scan failed");
        return false;
    }
    POOL_NEW_CLASS_ON_SHARED_PTR(
            _kernelPool.get(), _lineSegmentBuffer,
            LineSegmentBuffer, KHR_DEFAULT_BUFFER_COUNT, 1, _kernelPool.get());
    return true;
}

bool KhronosDataPointScan::parseQueryParamsFromCondition() {
    KhronosDataPointConditionVisitor visitor(_kernelPool.get(), _indexInfos);
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

    SQL_LOG(TRACE2, "data point scan condition: tsRange=[%ld, %ld) tagk={%s}",
            _tsRange.begin, _tsRange.end, StringUtil::toString(_tagKVInfos).c_str());

    return true;
}

bool KhronosDataPointScan::extractUsedField(OutputFieldsVisitor &visitor) {
    auto &usedFieldsColumnSet = visitor.getUsedFieldsColumnSet();
    for (auto &item : usedFieldsColumnSet) {
        if (KhronosUtil::typeMatch(_indexInfos, item, KHRONOS_TIMESTAMP_TYPE)) {
            if (!_tsColName.empty()) {
                SQL_LOG(ERROR, "duplicate timestamp field name: [%s] vs [%s]",
                        _tsColName.c_str(), item.c_str());
                return false;
            }
            _tsColName = item;
        } else if (KhronosUtil::typeMatch(_indexInfos, item, KHRONOS_WATERMARK_TYPE)) {
            if (!_watermarkColName.empty()) {
                SQL_LOG(ERROR, "only one watermark field is supported: [%s] vs [%s]",
                        _watermarkColName.c_str(), item.c_str());
                return false;
            }
            _watermarkColName = item;
        } else if (KhronosUtil::typeMatch(_indexInfos, item, KHRONOS_VALUE_TYPE)) {
            if (!_valueColName.empty()) {
                SQL_LOG(ERROR, "only one value field is supported: [%s] vs [%s]",
                        _valueColName.c_str(), item.c_str());
                return false;
            }
            _valueColName = item;
        } else {
            SQL_LOG(ERROR, "field [%s] is not supported in khronos data point scan",
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

    SQL_LOG(TRACE2, "data point scan used fields: ts=[%s], watermark=[%s], value=[%s], tagk=[%s]",
            _tsColName.c_str(), _watermarkColName.c_str(), _valueColName.c_str(),
            StringUtil::toString(_usedFieldsTagkSet.begin(), _usedFieldsTagkSet.end()).c_str());
    return true;
}

bool KhronosDataPointScan::doBatchScanImpl(TablePtr &table, bool &eof) {
    assert(_lineSegmentBuffer != nullptr);
    assert(_rowIter != nullptr);

    eof = false;
    table.reset(new Table(_kernelPool));

    ColumnData<KHR_TS_TYPE> *tsColumnData = nullptr;
    ColumnData<KHR_WM_TYPE> *watermarkColumnData = nullptr;
    ColumnData<KHR_VALUE_TYPE> *valueColumnData = nullptr;
    TagkColumnPackMap tagkColumnPackMap;
    if (!declareTableColumn(table, tsColumnData, watermarkColumnData,
                            valueColumnData, tagkColumnPackMap)) {
        SQL_LOG(ERROR, "declare table column failed");
        return false;
    }

    size_t batchSize = getBatchSize(table);
    uint64_t lastHash = 0;
    auto &lineSegmentBuffer = *_lineSegmentBuffer;
    int32_t errorCode = 0;
    while (batchSize > 0 && _limit > 0) {
        if (unlikely(lineSegmentBuffer.size() == _bufOffset)) {
            if (unlikely(_timeoutTerminator && _timeoutTerminator->checkTimeout())) {
                ON_KHRONOS_SCAN_TERMINATOR_TIMEOUT("run timeout");
                reportSimpleQps("dataScanTimeoutQps");
                return false;
            }

	    size_t nums = _rowIter->Next(lineSegmentBuffer, errorCode);
            _khronosDataScanInfo.set_callnextcount(_khronosDataScanInfo.callnextcount() + 1);
            if (errorCode != 0)
	    {
	        SQL_LOG(ERROR, "caught IO exception, errorMsg=[%s]", _rowIter->GetLastError().c_str());
		return false;
            }
            if (nums == 0) {
                eof = true;
                break;
            }
            _khronosDataScanInfo.set_pointscancount(
                _khronosDataScanInfo.pointscancount() + lineSegmentBuffer.size());
            _bufOffset = 0;
        }
        uint64_t curHash = lineSegmentBuffer.getSeriesHash();
        if (curHash != lastHash) {
            lastHash = curHash;
            string seriesKey = lineSegmentBuffer.getSeriesKey();
            // update value cache when sk changed
            if (!updateTagkColumnValueCache(seriesKey, tagkColumnPackMap, true)) {
                SQL_LOG(ERROR, "update tagk column value cache failed");
                return false;
            }
        }

        for (; batchSize > 0 && _limit > 0 && _bufOffset < lineSegmentBuffer.size();
             --batchSize, --_limit, ++_bufOffset)
        {
            auto &dataPoint = lineSegmentBuffer[_bufOffset];
            Row tableRow = table->allocateRow();
            if (tsColumnData != nullptr) {
                tsColumnData->set(tableRow, dataPoint.timestamp);
            }
            if (watermarkColumnData != nullptr) {
                watermarkColumnData->set(tableRow, _watermark);
            }
            assert(valueColumnData != nullptr);
            valueColumnData->set(tableRow, dataPoint.values[0]);

            for (auto &pair : tagkColumnPackMap) {
                pair.second.columnData->set(tableRow, pair.second.columnValueCache);
            }
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

bool KhronosDataPointScan::declareTableColumn(TablePtr &table,
        ColumnData<KHR_TS_TYPE> *&tsColumnData,
        ColumnData<KHR_WM_TYPE> *&watermarkColumnData,
        ColumnData<KHR_VALUE_TYPE> *&valueColumnData,
        TagkColumnPackMap &tagkColumnPackMap)
{
    tsColumnData = nullptr;
    if (!_tsColName.empty()) {
        string stripedTsColName = _tsColName;
        KernelUtil::stripName(stripedTsColName);
        tsColumnData = TableUtil::declareAndGetColumnData<KHR_TS_TYPE>(
                table, stripedTsColName);
        if (tsColumnData == nullptr) {
            SQL_LOG(ERROR, "declare timestamp column [%s] failed",
                    stripedTsColName.c_str());
            return false;
        }
    }
    if (!_watermarkColName.empty()) {
        string stripedWatermarkColName = _watermarkColName;
        KernelUtil::stripName(stripedWatermarkColName);
        watermarkColumnData = TableUtil::declareAndGetColumnData<KHR_WM_TYPE>(
                table, stripedWatermarkColName);
    }
    {
        assert(!_valueColName.empty());
        string stripedValueColName = _valueColName;
        KernelUtil::stripName(stripedValueColName);
        valueColumnData = TableUtil::declareAndGetColumnData<KHR_VALUE_TYPE>(
                table, stripedValueColName);
        if (valueColumnData == nullptr) {
            SQL_LOG(ERROR, "declare value column [%s] failed", stripedValueColName.c_str());
            return false;
        }
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

size_t KhronosDataPointScan::getBatchSize(TablePtr &table) {
    if (_batchSize == 0) {
        _batchSize = DEFAULT_BATCH_COUNT;
        uint32_t docSize = table->getRowSize();
        if (docSize != 0) {
            _batchSize = DEFAULT_BATCH_SIZE / docSize;
            if (_batchSize > (1 << 18)) { // max 256K
                _batchSize = 1 << 18;
            }
        }
    }
    return _batchSize;
}

END_HA3_NAMESPACE(sql);
