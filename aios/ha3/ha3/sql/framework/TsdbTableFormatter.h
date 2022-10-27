#pragma once
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/ErrorResult.h>
#include <ha3/sql/framework/SqlQueryResponse.h>
#include <ha3/sql/framework/TsdbResult.h>
#include <ha3/sql/data/Table.h>
#include <iquan_common/service/ResponseWrapper.h>

BEGIN_HA3_NAMESPACE(sql);

class TsdbTableFormatter
{
public:
    TsdbTableFormatter();
    ~TsdbTableFormatter();
    TsdbTableFormatter(const TsdbTableFormatter &) = delete;
    TsdbTableFormatter& operator=(const TsdbTableFormatter &) = delete;
public:
    static HA3_NS(common)::ErrorResult convertToTsdb(const TablePtr &table,
            const std::string &formatDesc,
            TsdbResults &tsdbResults);
    static HA3_NS(common)::ErrorResult convertToTsdb(const TablePtr &table,
            const std::string &formatDesc,
            float integrity,
            TsdbResults &tsdbResults);
private:
    static HA3_NS(common)::ErrorResult convertTable(const TablePtr &table,
            std::map<std::string, std::string> &tsdbFieldsMap,
            TsdbResults &tsdbResults);
    static HA3_NS(common)::ErrorResult convertTable(const TablePtr &table,
            std::map<std::string, std::string> &tsdbFieldsMap,
            float integrity,
            TsdbResults &tsdbResults,
            size_t memoryLimit=SQL_TSDB_RESULT_MEM_LIMIT);

    static bool parseTsdbFormatDesc(const std::string &formatDesc,
            std::map<std::string, std::string> &tsdbFieldsMap);
private:
    static const std::string SQL_TSDB_DPS;
    static const std::string SQL_TSDB_METRIC;
    static const std::string SQL_TSDB_SUMMARY;
    static const std::string SQL_TSDB_MESSAGE_WATERMARK;
    static const std::string SQL_TSDB_INTEGRITY;
    static const std::string SQL_TSDB_PAIR_SEP;
    static const std::string SQL_TSDB_KV_SEP;
    static std::set<std::string> tsdbKeySet;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(TsdbTableFormatter);
END_HA3_NAMESPACE(sql);
