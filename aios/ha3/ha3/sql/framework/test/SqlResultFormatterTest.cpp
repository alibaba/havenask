#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/framework/SqlResultFormatter.h>
#include <ha3/sql/ops/test/OpTestBase.h>
#include <khronos_table_interface/CommonDefine.h>
#include <khronos_table_interface/DataSeries.hpp>
#include <autil/legacy/RapidJsonHelper.h>

using namespace std;
using namespace khronos;
using namespace testing;
using namespace matchdoc;
using namespace autil;
using namespace autil_rapidjson;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(service);
BEGIN_HA3_NAMESPACE(sql);

class SqlResultFormatterTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
private:
    void createDataStr(const vector<KHR_TS_TYPE> &timestamps,
                       const vector<KHR_VALUE_TYPE> &values,
                       std::string &dataStr) {
        ASSERT_EQ(timestamps.size(), values.size());
        DataSeriesWriteOnly dataSeries;
        for (size_t i = 0; i < timestamps.size(); i++) {
            dataSeries.append(DataPoint(timestamps[i], values[i]));
        }
        MultiChar mc;
        autil::mem_pool::Pool pool;
        ASSERT_TRUE(dataSeries.serialize(&pool, mc));
        dataStr = {mc.data(), mc.size()};
    }

    void createTsdbDataByDataStr(const std::string &dataStr,
                                 MultiChar &tsdbData) {
        char* buf = MultiValueCreator::createMultiValueBuffer<char>(
                dataStr.data(), dataStr.size(), &_pool);
        tsdbData.init(buf);
    }

    TablePtr createSimpleTable() {
        MatchDocAllocatorPtr allocator;
        const auto &docs = getMatchDocs(allocator, 1);
        extendMatchDocAllocator(allocator, docs, "value", {string("a")});
        return TablePtr(new Table(docs, allocator));
    }

    void checkJsonStringEqual(const string &expected, const string &actual) {
        AutilPoolAllocator expectedAllocator(&_pool);
        SimpleDocument expectedDoc(&expectedAllocator);
        expectedDoc.Parse(expected.c_str());
        ASSERT_FALSE(expectedDoc.HasParseError());

        AutilPoolAllocator actualAllocator(&_pool);
        SimpleDocument actualDoc(&actualAllocator);
        actualDoc.Parse(actual.c_str());
        ASSERT_FALSE(actualDoc.HasParseError());

        ASSERT_TRUE(expectedDoc == actualDoc)
            << "expected:" << std::endl
            << RapidJsonHelper::SimpleValue2Str(expectedDoc) << std::endl
            << "actual:" << std::endl
            << RapidJsonHelper::SimpleValue2Str(actualDoc) << std::endl;
    }
private:
    autil::mem_pool::Pool _pool;
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(table, SqlResultFormatterTest);

void SqlResultFormatterTest::setUp() {
}

void SqlResultFormatterTest::tearDown() {
}

TEST_F(SqlResultFormatterTest, testCalcCoveredPercent) {
    {
        SqlSearchInfo sqlSearchInfo;
        double res = SqlResultFormatter::calcCoveredPercent(sqlSearchInfo);
        ASSERT_DOUBLE_EQ(0.0, res);
    }
    {
        SqlSearchInfo sqlSearchInfo;
        RpcInfo rpcInfo;
        rpcInfo.set_lackcount(9);
        rpcInfo.set_totalrpccount(10);
        *(sqlSearchInfo.add_rpcinfos()) = rpcInfo;
        double res = SqlResultFormatter::calcCoveredPercent(sqlSearchInfo);
        ASSERT_DOUBLE_EQ(0.1, res);
    }
    {
        SqlSearchInfo sqlSearchInfo;
        RpcInfo rpcInfo1;
        rpcInfo1.set_lackcount(9);
        rpcInfo1.set_totalrpccount(10);
        *(sqlSearchInfo.add_rpcinfos()) = rpcInfo1;

        RpcInfo rpcInfo2;
        rpcInfo2.set_lackcount(10);
        rpcInfo2.set_totalrpccount(9);
        *(sqlSearchInfo.add_rpcinfos()) = rpcInfo2;
        double res = SqlResultFormatter::calcCoveredPercent(sqlSearchInfo);
        ASSERT_DOUBLE_EQ(1.0/19.0, res);
    }

    {
        SqlSearchInfo sqlSearchInfo;
        RpcInfo rpcInfo1;
        rpcInfo1.set_lackcount(0);
        rpcInfo1.set_totalrpccount(0);
        *(sqlSearchInfo.add_rpcinfos()) = rpcInfo1;

        RpcInfo rpcInfo2;
        rpcInfo2.set_lackcount(0);
        rpcInfo2.set_totalrpccount(0);
        *(sqlSearchInfo.add_rpcinfos()) = rpcInfo2;
        double res = SqlResultFormatter::calcCoveredPercent(sqlSearchInfo);
        ASSERT_DOUBLE_EQ(0.0, res);
    }
}

TEST_F(SqlResultFormatterTest, testFormatTsdb) {
    QrsSessionSqlResult sqlResult;
    sqlResult.formatDesc = "dps#dps_new";
    sqlResult.formatType = QrsSessionSqlResult::SQL_RF_TSDB;
    sqlResult.sqlQuery = "select a from b";

    MatchDocAllocatorPtr allocator;
    const auto &docs = getMatchDocs(allocator, 1);
    MultiChar tsdbData1;
    string dataStr1;
    createDataStr({0}, {0.1}, dataStr1);
    createTsdbDataByDataStr(dataStr1, tsdbData1);
    extendMatchDocAllocator<MultiChar>(allocator, docs, "dps_new", {tsdbData1});
    sqlResult.table = TablePtr(new Table(docs, allocator));

    // test search info
    SqlSearchInfo sqlSearchInfo;
    RpcInfo rpcInfo;
    rpcInfo.set_lackcount(9);
    rpcInfo.set_totalrpccount(10);
    *(sqlSearchInfo.add_rpcinfos()) = rpcInfo;
    auto *scanInfo = sqlSearchInfo.add_scaninfos();
    scanInfo->set_kernelname("test");
    SqlAccessLog accessLog;
    accessLog._searchInfo = sqlSearchInfo;
    sqlResult.getSearchInfo = true;

    SqlResultFormatter::format(sqlResult, &accessLog, nullptr);
    // use FastToJsonString
    string expectStr = R"json({
    "covered_percent": 0.1,
    "error_info": {
        "Error": "ERROR_NONE",
        "ErrorCode": 0,
        "Message": ""
    },
    "format_type": "tsdb",
    "iquan_plan": {
        "error_code": 0,
        "error_message": "",
        "result": {
            "exec_params": {},
            "rel_plan": [],
            "rel_plan_version": ""
        }
    },
    "navi_graph": "",
    "row_count": 0,
    "search_info": {
        "rpcInfos": [
            {
                "lackCount": 9,
                "totalRpcCount": 10
            }
        ],
        "scanInfos": [
            {
                "kernelName": "test"
            }
        ]
    },
    "sql_query": "select a from b",
    "sql_result": [
        {
            "dps": {
                "0": 0.10000000149011612
            },
            "integrity": 0.10000000149011612,
            "messageWatermark": -1,
            "metric": "",
            "summary": -1.0,
            "tags": {}
        }
    ],
    "total_time": 0.0,
    "trace": []
})json";
    ASSERT_NO_FATAL_FAILURE(checkJsonStringEqual(expectStr, sqlResult.resultStr));
}

TEST_F(SqlResultFormatterTest, DISABLED_testFormatTsdbPerf) {
    QrsSessionSqlResult sqlResult;
    sqlResult.formatDesc = "dps#dps_new";
    sqlResult.formatType = QrsSessionSqlResult::SQL_RF_TSDB;
    sqlResult.sqlQuery = "select a from b";

    MatchDocAllocatorPtr allocator;
    const auto &docs = getMatchDocs(allocator, 1);
    MultiChar tsdbData1;
    string dataStr1;

    const size_t recordNums = 10000;
    vector<KHR_TS_TYPE> timestamps;
    vector<KHR_VALUE_TYPE> values;
    for (size_t i = 0; i < recordNums; ++i) {
        timestamps.emplace_back(1586919037 + 20 * i);
        values.emplace_back(0.5 + 0.1 * i);
    }

    createDataStr(timestamps, values, dataStr1);
    createTsdbDataByDataStr(dataStr1, tsdbData1);
    extendMatchDocAllocator<MultiChar>(allocator, docs, "dps_new", {tsdbData1});
    sqlResult.table = TablePtr(new Table(docs, allocator));

    uint64_t beginTs = TimeUtility::currentTime();
    const size_t round = 2000;
    for (size_t i = 0; i < round; ++i) {
        if (i % (round / 10)  == 0) {
            std::cout << "round " << i << std::endl;
        }
        SqlResultFormatter::format(sqlResult, nullptr, nullptr);
    }
    std::cout << "cost " << TimeUtility::currentTime() - beginTs << "us" << std::endl;
}

TEST_F(SqlResultFormatterTest, testFormatTsdbPrometheus) {
    QrsSessionSqlResult sqlResult;
    sqlResult.formatDesc = "dps#dps_new";
    sqlResult.formatType = QrsSessionSqlResult::SQL_RF_TSDB_PROMETHEUS;
    sqlResult.sqlQuery = "select a from b";

    MatchDocAllocatorPtr allocator;
    const auto &docs = getMatchDocs(allocator, 1);
    MultiChar tsdbData1;
    string dataStr1;
    createDataStr({0}, {0.1}, dataStr1);
    createTsdbDataByDataStr(dataStr1, tsdbData1);
    extendMatchDocAllocator<MultiChar>(allocator, docs, "dps_new", {tsdbData1});
    extendMatchDocAllocator(allocator, docs, "metric", {string("m1")});
    extendMatchDocAllocator(allocator, docs, "tagk1", {string("a")});
    extendMatchDocAllocator(allocator, docs, "tagk2", {string("A")});
    sqlResult.table = TablePtr(new Table(docs, allocator));

    // test search info
    SqlSearchInfo sqlSearchInfo;
    RpcInfo rpcInfo;
    rpcInfo.set_lackcount(9);
    rpcInfo.set_totalrpccount(10);
    *(sqlSearchInfo.add_rpcinfos()) = rpcInfo;
    auto *scanInfo = sqlSearchInfo.add_scaninfos();
    scanInfo->set_kernelname("test");
    SqlAccessLog accessLog;
    accessLog._searchInfo = sqlSearchInfo;
    sqlResult.getSearchInfo = true;

    string expectStr = R"json({
    "covered_percent": 0.1,
    "data": {
        "result": [
            {
                "integrity": 0.10000000149011612,
                "messageWatermark": -1,
                "metric": {
                    "__name__": "m1",
                    "tagk1": "a",
                    "tagk2": "A"
                },
                "summary": -1.0,
                "values": [
                    [
                        0,
                        "0.100000"
                    ]
                ]
            }
        ],
        "resultType": "matrix"
    },
    "error": "",
    "errorType": "ERROR_NONE",
    "format_type": "prometheus",
    "iquan_plan": {
        "error_code": 0,
        "error_message": "",
        "result": {
            "exec_params": {},
            "rel_plan": [],
            "rel_plan_version": ""
        }
    },
    "navi_graph": "",
    "row_count": 0,
    "search_info": {
        "rpcInfos": [
            {
                "lackCount": 9,
                "totalRpcCount": 10
            }
        ],
        "scanInfos": [
            {
                "kernelName": "test"
            }
        ]
    },
    "sql_query": "select a from b",
    "status": "success",
    "total_time": 0.0,
    "trace": []
})json";
    SqlResultFormatter::format(sqlResult, &accessLog, nullptr);
    ASSERT_NO_FATAL_FAILURE(checkJsonStringEqual(expectStr, sqlResult.resultStr));
}

TEST_F(SqlResultFormatterTest, testFormatFullJson) {
    QrsSessionSqlResult sqlResult;
    sqlResult.formatType = QrsSessionSqlResult::SQL_RF_FULL_JSON;
    sqlResult.sqlQuery = "select a from b";
    sqlResult.table = createSimpleTable();
    SqlSearchInfo sqlSearchInfo;
    RpcInfo rpcInfo;
    rpcInfo.set_lackcount(9);
    rpcInfo.set_totalrpccount(10);
    *(sqlSearchInfo.add_rpcinfos()) = rpcInfo;
    SqlAccessLog accessLog;
    accessLog._searchInfo = sqlSearchInfo;
    SqlResultFormatter::format(sqlResult, &accessLog, nullptr);
    string expectStr = R"json({
    "error_info": {
        "Error": "ERROR_NONE",
        "ErrorCode": 0,
        "Message": ""
    },
    "format_type": "full_json",
    "iquan_plan": {
        "error_code": 0,
        "error_message": "",
        "result": {
            "exec_params": {},
            "rel_plan": [],
            "rel_plan_version": ""
        }
    },
    "row_count": 0,
    "search_info": {},
    "sql_query": "select a from b",
    "sql_result": {
        "column_name": [
            "value"
        ],
        "column_type": [
            "multi_char"
        ],
        "data": [
            [
                "a"
            ]
        ]
    },
    "covered_percent":0.1,
    "total_time": 0,
    "navi_graph":"",
    "trace": []
})json";
    ASSERT_NO_FATAL_FAILURE(checkJsonStringEqual(expectStr, sqlResult.resultStr));
}
TEST_F(SqlResultFormatterTest, testFormatJson) {
    QrsSessionSqlResult sqlResult;
    sqlResult.formatType = QrsSessionSqlResult::SQL_RF_JSON;
    sqlResult.sqlQuery = "select a from b";
    sqlResult.table = createSimpleTable();
    SqlSearchInfo sqlSearchInfo;
    RpcInfo rpcInfo;
    rpcInfo.set_lackcount(9);
    rpcInfo.set_totalrpccount(10);
    *(sqlSearchInfo.add_rpcinfos()) = rpcInfo;
    SqlAccessLog accessLog;
    accessLog._searchInfo = sqlSearchInfo;
    SqlResultFormatter::format(sqlResult, &accessLog, nullptr);
    string expectStr = R"json({
    "error_info": "{\"Error\":ERROR_NONE}",
    "format_type": "json",
    "iquan_plan": {
        "error_code": 0,
        "error_message": "",
        "result": {
            "exec_params": {},
            "rel_plan": [],
            "rel_plan_version": ""
        }
    },
    "row_count": 0,
    "search_info": {},
    "sql_query": "select a from b",
    "sql_result": "{\"data\":[[\"a\"]],\"column_name\":[\"value\"],\"column_type\":[\"multi_char\"]}",
    "total_time": 0.0,
    "covered_percent":0.1,
    "navi_graph":"",
    "trace": []
})json";
    ASSERT_NO_FATAL_FAILURE(checkJsonStringEqual(expectStr, sqlResult.resultStr));
}
TEST_F(SqlResultFormatterTest, testFormatString) {
    QrsSessionSqlResult sqlResult;
    sqlResult.formatType = QrsSessionSqlResult::SQL_RF_STRING;
    sqlResult.sqlQuery = "select a from b";
    sqlResult.table = createSimpleTable();
    SqlResultFormatter::format(sqlResult, nullptr, nullptr);
    string expectStr = "\n------------------------------- TABLE INFO ---------------------------\n           value(multi_char) |\n                           a |\n\n------------------------------- PLAN INFO ---------------------------\nSQL QUERY:\nselect a from b\nIQUAN_RESULT:\n{\"error_code\":0,\"error_message\":\"\",\"result\":{\"exec_params\":{},\"rel_plan\":[],\"rel_plan_version\":\"\"}}\n\n------------------------------- TRACE INFO ---------------------------\n";
    ASSERT_EQ(expectStr, sqlResult.resultStr);
}
END_HA3_NAMESPACE(sql);
