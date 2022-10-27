#pragma once
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/ErrorResult.h>
#include <ha3/sql/framework/SqlQueryResponse.h>
#include <ha3/sql/framework/PrometheusResult.h>
#include <ha3/sql/data/Table.h>
#include <iquan_common/service/ResponseWrapper.h>

BEGIN_HA3_NAMESPACE(sql);

class PrometheusTableFormatter
{
public:
    PrometheusTableFormatter();
    ~PrometheusTableFormatter();
    PrometheusTableFormatter(const PrometheusTableFormatter &) = delete;
    PrometheusTableFormatter& operator=(const PrometheusTableFormatter &) = delete;
public:
    static HA3_NS(common)::ErrorResult convertToPrometheus(const TablePtr &table,
            const std::string &formatDesc,
            PrometheusResults &prometheusResults);
    static HA3_NS(common)::ErrorResult convertToPrometheus(const TablePtr &table,
            const std::string &formatDesc,
            float integrity,
            PrometheusResults &prometheusResults);
private:
    static HA3_NS(common)::ErrorResult convertTable(const TablePtr &table,
            std::map<std::string, std::string> &prometheusFieldsMap,
            PrometheusResults &prometheusResults);
    static HA3_NS(common)::ErrorResult convertTable(const TablePtr &table,
            std::map<std::string, std::string> &prometheusFieldsMap,
            float integrity,
            PrometheusResults &prometheusResults,
            size_t memoryLimit=SQL_TSDB_RESULT_MEM_LIMIT);

    static bool parsePrometheusFormatDesc(const std::string &formatDesc,
            std::map<std::string, std::string> &prometheusFieldsMap);
private:
    static const std::string SQL_PROMETHEUS_DPS;
    static const std::string SQL_PROMETHEUS_METRIC;
    static const std::string SQL_PROMETHEUS_SUMMARY;
    static const std::string SQL_PROMETHEUS_MESSAGE_WATERMARK;
    static const std::string SQL_PROMETHEUS_INTEGRITY;
    static const std::string SQL_PROMETHEUS_PAIR_SEP;
    static const std::string SQL_PROMETHEUS_KV_SEP;
    static std::set<std::string> prometheusKeySet;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(PrometheusTableFormatter);
END_HA3_NAMESPACE(sql);
