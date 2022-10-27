#include <ha3/sql/framework/PrometheusTableFormatter.h>
#include <khronos_table_interface/CommonDefine.h>
#include <khronos_table_interface/DataSeries.hpp>
#include <ha3/sql/data/TableUtil.h>

using namespace std;
using namespace autil;
using namespace khronos;
using namespace autil::legacy;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, PrometheusTableFormatter);

const string PrometheusTableFormatter::SQL_PROMETHEUS_PAIR_SEP = ",";
const string PrometheusTableFormatter::SQL_PROMETHEUS_KV_SEP = "#";

const string PrometheusTableFormatter::SQL_PROMETHEUS_DPS = "dps";
const string PrometheusTableFormatter::SQL_PROMETHEUS_METRIC = "metric";
const string PrometheusTableFormatter::SQL_PROMETHEUS_SUMMARY = "summary";
const string PrometheusTableFormatter::SQL_PROMETHEUS_MESSAGE_WATERMARK = "messageWatermark";
const string PrometheusTableFormatter::SQL_PROMETHEUS_INTEGRITY = "integrity";

set<string> PrometheusTableFormatter::prometheusKeySet = {SQL_PROMETHEUS_METRIC, SQL_PROMETHEUS_MESSAGE_WATERMARK,
                                                          SQL_PROMETHEUS_SUMMARY, SQL_PROMETHEUS_INTEGRITY, SQL_PROMETHEUS_DPS};

PrometheusTableFormatter::PrometheusTableFormatter() {
}

PrometheusTableFormatter::~PrometheusTableFormatter() {
}

ErrorResult PrometheusTableFormatter::convertToPrometheus(const TablePtr &table, const string &formatDesc,
        PrometheusResults &prometheusResults)
{
    float integrity = -1;
    return PrometheusTableFormatter::convertToPrometheus(table, formatDesc, integrity, prometheusResults);
}

ErrorResult PrometheusTableFormatter::convertToPrometheus(const TablePtr &table, const string &formatDesc,
        float integrity, PrometheusResults &prometheusResults)
{
    map<string, string> prometheusFieldsMap;
    if (!parsePrometheusFormatDesc(formatDesc, prometheusFieldsMap)) {
        ErrorResult errorResult;
        errorResult.resetError(ERROR_SQL_PROMETHEUS_FORMAT, "parse prometheus format error: " + formatDesc);
        return errorResult;
    }
    return convertTable(table, prometheusFieldsMap, integrity, prometheusResults);
}

ErrorResult PrometheusTableFormatter::convertTable(const TablePtr &table,
        std::map<std::string, std::string> &prometheusFieldsMap,
        PrometheusResults &prometheusResults)
{
    float integrity = -1;
    return PrometheusTableFormatter::convertTable(table, prometheusFieldsMap, integrity, prometheusResults);
}

ErrorResult PrometheusTableFormatter::convertTable(const TablePtr &table,
        std::map<std::string, std::string> &prometheusFieldsMap,
        float integrity, PrometheusResults &prometheusResults,
        size_t memoryLimit)
{
    assert(table != nullptr);
    ErrorResult errorResult;
    set<string> valueSets;
    string dpsColName = SQL_PROMETHEUS_DPS;
    auto iter = prometheusFieldsMap.find(SQL_PROMETHEUS_DPS);
    if (iter != prometheusFieldsMap.end()) {
        dpsColName = iter->second;
    }
    valueSets.insert(dpsColName);
    ColumnData<autil::MultiChar> * dpsColumn =
        TableUtil::getColumnData<autil::MultiChar>(table, dpsColName);
    if (!dpsColumn) {
        errorResult.resetError(ERROR_SQL_PROMETHEUS_TO_JSON, "get dps column(" + dpsColName+ ") failed");
        return errorResult;
    }

    string metricColName = SQL_PROMETHEUS_METRIC;
    iter = prometheusFieldsMap.find(SQL_PROMETHEUS_METRIC);
    ColumnData<autil::MultiChar> * metricColumn = nullptr; // metric column can empty
    if (iter != prometheusFieldsMap.end()) {
        metricColName = iter->second;
    }
    metricColumn = TableUtil::getColumnData<autil::MultiChar>(table, metricColName);
    if (iter != prometheusFieldsMap.end() && !metricColumn) {
        errorResult.resetError(ERROR_SQL_PROMETHEUS_TO_JSON, "get metric column(" + metricColName+ ") failed");
        return errorResult;
    }

    valueSets.insert(metricColName);

    string summaryColName = SQL_PROMETHEUS_SUMMARY;
    iter = prometheusFieldsMap.find(SQL_PROMETHEUS_SUMMARY);
    ColumnData<double> * summaryColumn = nullptr; // summary column can empty
    if (iter != prometheusFieldsMap.end()) {
        summaryColName = iter->second;
    }
    summaryColumn = TableUtil::tryGetTypedColumnData<double>(table, summaryColName);
    if (iter != prometheusFieldsMap.end() && !summaryColumn) {
        errorResult.resetError(ERROR_SQL_PROMETHEUS_TO_JSON, "get summary column(" + summaryColName+ ") failed");
        return errorResult;
    }
    valueSets.insert(summaryColName);

    string integrityColName = SQL_PROMETHEUS_INTEGRITY;
    iter = prometheusFieldsMap.find(SQL_PROMETHEUS_INTEGRITY);
    ColumnData<float> * integrityColumn = nullptr; // integrity column can empty
    if (iter != prometheusFieldsMap.end()) {
        integrityColName = iter->second;
    }
    integrityColumn = TableUtil::tryGetTypedColumnData<float>(table, integrityColName);
    if (iter != prometheusFieldsMap.end() && !integrityColumn) {
        errorResult.resetError(ERROR_SQL_PROMETHEUS_TO_JSON, "get integrity column(" + integrityColName+ ") failed");
        return errorResult;
    }
    valueSets.insert(integrityColName);

    string watermarkColName = SQL_PROMETHEUS_MESSAGE_WATERMARK;
    iter = prometheusFieldsMap.find(SQL_PROMETHEUS_MESSAGE_WATERMARK);
    ColumnData<KHR_WM_TYPE> * watermarkColumn = nullptr; // watermark column can empty
    if (iter != prometheusFieldsMap.end()) {
        watermarkColName = iter->second;
    }
    watermarkColumn = TableUtil::getColumnData<KHR_WM_TYPE>(table, watermarkColName);
    if (iter != prometheusFieldsMap.end() && !watermarkColumn) {
        errorResult.resetError(ERROR_SQL_PROMETHEUS_TO_JSON, "get watermark column(" + watermarkColName+ ") failed");
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
    PrometheusResults tmpRes;
    size_t estimatedMem = 0;
    for (size_t row = 0; row < rowCount; row++) {
        if (estimatedMem > memoryLimit) {
            errorResult.resetError(
                    ERROR_SQL_PROMETHEUS_TO_JSON,
                    "too many output result, current estimated memory [" +
                    to_string(estimatedMem) + "] > [" + to_string(memoryLimit) +"]");
            return errorResult;
        }

        PrometheusResult prometheusRow;
        const autil::MultiChar& dpsData = dpsColumn->get(row);
        uint32_t dataSize = dpsData.size();
        const char *buf = dpsData.data();
        DataSeriesReadOnly dataSeries(buf, dataSize);
        if (!dataSeries.isValid()) {
            errorResult.resetError(ERROR_SQL_PROMETHEUS_TO_JSON, "invalid prometheus data(" + string(buf, dataSize) + ")");
            return errorResult;
        }
        DataSeriesReadOnly::iterator iter = dataSeries.begin();
        for (;iter != dataSeries.end(); iter ++) {
            KHR_TS_TYPE ts = iter->getTimeStamp();
            KHR_VALUE_TYPE tsVal = iter->getValue();
            prometheusRow.values.emplace_back((vector<Any>){Any(ts), Any(to_string(tsVal))});
        }
        estimatedMem += (10 + 10) * prometheusRow.values.size();
        if (metricColumn) {
            const auto &multiStr = metricColumn->get(row);
            prometheusRow.metric["__name__"] = string(multiStr.data(), multiStr.size());
            estimatedMem += multiStr.size();
        }
        if (watermarkColumn) {
            prometheusRow.messageWatermark = watermarkColumn->get(row);
        }
        if (summaryColumn) {
            prometheusRow.summary = summaryColumn->get(row);
        }
        if (integrityColumn) {
            prometheusRow.integrity = integrityColumn->get(row);
        } else {
            prometheusRow.integrity = integrity;
        }
        for (size_t i = 0; i < tagColumnNames.size(); i++) {
            auto &colName = tagColumnNames[i];
            string tagValue = table->getColumn(tagColumnIds[i])->toString(row);
            estimatedMem += tagValue.size() + colName.size();
            prometheusRow.metric[colName] = tagValue;
        }
        tmpRes.emplace_back(std::move(prometheusRow));
    }
    prometheusResults = std::move(tmpRes);
    return errorResult;
}

bool PrometheusTableFormatter::parsePrometheusFormatDesc(const string &formatDesc,
        map<string, string> &prometheusFieldsMap)
{
    const vector<string> &pairVec = StringUtil::split(formatDesc, SQL_PROMETHEUS_PAIR_SEP);
    for (auto pairStr : pairVec) {
        const vector<string> &kv = StringUtil::split(pairStr, SQL_PROMETHEUS_KV_SEP);
        if (kv.size() != 2) {
            return false;
        }
        if (prometheusKeySet.count(kv[0]) == 1) {
            prometheusFieldsMap[kv[0]] = kv[1];
        }
    }
    return true;
}


END_HA3_NAMESPACE(sql);
