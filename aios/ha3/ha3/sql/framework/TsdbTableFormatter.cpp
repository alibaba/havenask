#include <ha3/sql/framework/TsdbTableFormatter.h>
#include <khronos_table_interface/CommonDefine.h>
#include <khronos_table_interface/DataSeries.hpp>
#include <ha3/sql/data/TableUtil.h>

using namespace std;
using namespace autil;
using namespace khronos;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, TsdbTableFormatter);

const string TsdbTableFormatter::SQL_TSDB_PAIR_SEP = ",";
const string TsdbTableFormatter::SQL_TSDB_KV_SEP = "#";

const string TsdbTableFormatter::SQL_TSDB_DPS = "dps";
const string TsdbTableFormatter::SQL_TSDB_METRIC = "metric";
const string TsdbTableFormatter::SQL_TSDB_SUMMARY = "summary";
const string TsdbTableFormatter::SQL_TSDB_MESSAGE_WATERMARK = "messageWatermark";
const string TsdbTableFormatter::SQL_TSDB_INTEGRITY = "integrity";

set<string> TsdbTableFormatter::tsdbKeySet = {SQL_TSDB_METRIC, SQL_TSDB_MESSAGE_WATERMARK,
                                              SQL_TSDB_SUMMARY, SQL_TSDB_INTEGRITY, SQL_TSDB_DPS};

TsdbTableFormatter::TsdbTableFormatter() {
}

TsdbTableFormatter::~TsdbTableFormatter() {
}

ErrorResult TsdbTableFormatter::convertToTsdb(const TablePtr &table, const string &formatDesc,
        TsdbResults &tsdbResults)
{
    float integrity = -1;
    return TsdbTableFormatter::convertToTsdb(table, formatDesc, integrity, tsdbResults);
}

ErrorResult TsdbTableFormatter::convertToTsdb(const TablePtr &table, const string &formatDesc,
        float integrity, TsdbResults &tsdbResults)
{
    map<string, string> tsdbFieldsMap;
    if (!parseTsdbFormatDesc(formatDesc, tsdbFieldsMap)) {
        ErrorResult errorResult;
        errorResult.resetError(ERROR_SQL_TSDB_FORMAT, "parse tsdb format error: " + formatDesc);
        return errorResult;
    }
    return convertTable(table, tsdbFieldsMap, integrity, tsdbResults);
}

ErrorResult TsdbTableFormatter::convertTable(const TablePtr &table,
        std::map<std::string, std::string> &tsdbFieldsMap,
        TsdbResults &tsdbResults)
{
    float integrity = -1;
    return TsdbTableFormatter::convertTable(table, tsdbFieldsMap, integrity, tsdbResults);
}

ErrorResult TsdbTableFormatter::convertTable(const TablePtr &table,
        std::map<std::string, std::string> &tsdbFieldsMap,
        float integrity, TsdbResults &tsdbResults,
        size_t memoryLimit)
{
    assert(table != nullptr);
    ErrorResult errorResult;
    set<string> valueSets;
    string dpsColName = SQL_TSDB_DPS;
    auto iter = tsdbFieldsMap.find(SQL_TSDB_DPS);
    if (iter != tsdbFieldsMap.end()) {
        dpsColName = iter->second;
    }
    valueSets.insert(dpsColName);
    ColumnData<autil::MultiChar> * dpsColumn =
        TableUtil::getColumnData<autil::MultiChar>(table, dpsColName);
    if (!dpsColumn) {
        errorResult.resetError(ERROR_SQL_TSDB_TO_JSON, "get dps column(" + dpsColName+ ") failed");
        return errorResult;
    }

    string metricColName = SQL_TSDB_METRIC;
    iter = tsdbFieldsMap.find(SQL_TSDB_METRIC);
    ColumnData<autil::MultiChar> * metricColumn = nullptr; // metric column can empty
    if (iter != tsdbFieldsMap.end()) {
        metricColName = iter->second;
    }
    metricColumn = TableUtil::getColumnData<autil::MultiChar>(table, metricColName);
    if (iter != tsdbFieldsMap.end() && !metricColumn) {
        errorResult.resetError(ERROR_SQL_TSDB_TO_JSON, "get metric column(" + metricColName+ ") failed");
        return errorResult;
    }

    valueSets.insert(metricColName);

    string summaryColName = SQL_TSDB_SUMMARY;
    iter = tsdbFieldsMap.find(SQL_TSDB_SUMMARY);
    ColumnData<double> * summaryColumn = nullptr; // summary column can empty
    if (iter != tsdbFieldsMap.end()) {
        summaryColName = iter->second;
    }
    summaryColumn = TableUtil::tryGetTypedColumnData<double>(table, summaryColName);
    if (iter != tsdbFieldsMap.end() && !summaryColumn) {
        errorResult.resetError(ERROR_SQL_TSDB_TO_JSON, "get summary column(" + summaryColName+ ") failed");
        return errorResult;
    }
    valueSets.insert(summaryColName);

    string integrityColName = SQL_TSDB_INTEGRITY;
    iter = tsdbFieldsMap.find(SQL_TSDB_INTEGRITY);
    ColumnData<float> * integrityColumn = nullptr; // integrity column can empty
    if (iter != tsdbFieldsMap.end()) {
        integrityColName = iter->second;
    }
    integrityColumn = TableUtil::tryGetTypedColumnData<float>(table, integrityColName);
    if (iter != tsdbFieldsMap.end() && !integrityColumn) {
        errorResult.resetError(ERROR_SQL_TSDB_TO_JSON, "get integrity column(" + integrityColName+ ") failed");
        return errorResult;
    }
    valueSets.insert(integrityColName);

    string watermarkColName = SQL_TSDB_MESSAGE_WATERMARK;
    iter = tsdbFieldsMap.find(SQL_TSDB_MESSAGE_WATERMARK);
    ColumnData<KHR_WM_TYPE> * watermarkColumn = nullptr; // watermark column can empty
    if (iter != tsdbFieldsMap.end()) {
        watermarkColName = iter->second;
    }
    watermarkColumn = TableUtil::getColumnData<KHR_WM_TYPE>(table, watermarkColName);
    if (iter != tsdbFieldsMap.end() && !watermarkColumn) {
        errorResult.resetError(ERROR_SQL_TSDB_TO_JSON, "get watermark column(" + watermarkColName+ ") failed");
        return errorResult;
    }
    valueSets.insert(watermarkColName);

    size_t rowCount = table->getRowCount();
    size_t colCount = table->getColumnCount();
    vector<size_t> tagColumnIds;
    vector<string> tagColumnNames;
    for (size_t col = 0; col < colCount; col++) {
        const auto &colName = table->getColumnName(col);
        if (valueSets.count(colName) == 0) {
            tagColumnIds.push_back(col);
            tagColumnNames.push_back(colName);
        }
    }
    TsdbResults tmpRes;
    size_t estimatedMem = 0;
    for (size_t row = 0; row < rowCount; row++) {
        if (estimatedMem > memoryLimit) {
            errorResult.resetError(
                    ERROR_SQL_TSDB_TO_JSON,
                    "too many output result, current estimated memory [" +
                    to_string(estimatedMem) + "] > [" + to_string(memoryLimit) +"]");
            return errorResult;
        }

        TsdbResult tsdbRow;
        const autil::MultiChar& dpsData = dpsColumn->get(row);
        uint32_t dataSize = dpsData.size();
        const char *buf = dpsData.data();
        DataSeriesReadOnly dataSeries(buf, dataSize);
        if (!dataSeries.isValid()) {
            errorResult.resetError(ERROR_SQL_TSDB_TO_JSON, "invalid tsdb data(" + string(buf, dataSize) + ")");
            return errorResult;
        }
        DataSeriesReadOnly::iterator iter = dataSeries.begin();
        for (;iter != dataSeries.end(); iter ++) {
            KHR_TS_TYPE ts = iter->getTimeStamp();
            KHR_VALUE_TYPE tsVal = iter->getValue();
            const string &tsStr = autil::StringUtil::toString(ts);
            tsdbRow.dps[tsStr] = (float)tsVal;
        }
        estimatedMem += (10 + 10) * tsdbRow.dps.size();
        if (metricColumn) {
            const auto &multiStr = metricColumn->get(row);
            tsdbRow.metric = string(multiStr.data(), multiStr.size());
            estimatedMem += multiStr.size();
        }
        if (watermarkColumn) {
            tsdbRow.messageWatermark = watermarkColumn->get(row);
        }
        if (summaryColumn) {
            tsdbRow.summary = summaryColumn->get(row);
        }
        if (integrityColumn) {
            tsdbRow.integrity = integrityColumn->get(row);
        } else {
            tsdbRow.integrity = integrity;
        }
        for (size_t i = 0; i < tagColumnNames.size(); i++) {
            auto &colName = tagColumnNames[i];
            string tagValue = table->getColumn(tagColumnIds[i])->toString(row);
            estimatedMem += tagValue.size() + colName.size();
            tsdbRow.tags[colName] = tagValue;
        }
        tmpRes.push_back(tsdbRow);
    }
    tsdbResults = std::move(tmpRes);
    return errorResult;
}

bool TsdbTableFormatter::parseTsdbFormatDesc(const string &formatDesc,
        map<string, string> &tsdbFieldsMap)
{
    const vector<string> &pairVec = StringUtil::split(formatDesc, SQL_TSDB_PAIR_SEP);
    for (auto pairStr : pairVec) {
        const vector<string> &kv = StringUtil::split(pairStr, SQL_TSDB_KV_SEP);
        if (kv.size() != 2) {
            return false;
        }
        if (tsdbKeySet.count(kv[0]) == 1) {
            tsdbFieldsMap[kv[0]] = kv[1];
        }
    }
    return true;
}


END_HA3_NAMESPACE(sql);
