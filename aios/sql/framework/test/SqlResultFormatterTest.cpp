#include "sql/framework/SqlResultFormatter.h"

#include <algorithm>
#include <flatbuffers/flatbuffers.h>
#include <iostream>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/test/JsonTestUtil.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/proto/SqlResult_generated.h"
#include "ha3/proto/TwoDimTable_generated.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "multi_call/common/ErrorCode.h"
#include "multi_call/stream/GigStreamRpcInfo.h"
#include "navi/util/NaviTestPool.h"
#include "sql/framework/QrsSessionSqlResult.h"
#include "sql/framework/SqlAccessLog.h"
#include "sql/framework/SqlAccessLogFormatHelper.h"
#include "sql/proto/SqlSearch.pb.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "table/test/TableTestUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace matchdoc;
using namespace autil;
using namespace autil::legacy;
using namespace table;
using namespace multi_call;
using namespace isearch;

namespace sql {

class SqlResultFormatterTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

private:
    TablePtr createSimpleTable() {
        MatchDocAllocatorPtr allocator;
        const auto &docs = _matchDocUtil.createMatchDocs(allocator, 1);
        _matchDocUtil.extendMatchDocAllocator(allocator, docs, "value", {string("a")});
        return TablePtr(new Table(docs, allocator));
    }

    GigStreamRpcInfoMap constructLackRpcInfoMap() const;

private:
    autil::mem_pool::PoolAsan _pool;
    table::MatchDocUtil _matchDocUtil;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(sql, SqlResultFormatterTest);

void SqlResultFormatterTest::setUp() {}

void SqlResultFormatterTest::tearDown() {}

GigStreamRpcInfoMap SqlResultFormatterTest::constructLackRpcInfoMap() const {
    GigStreamRpcInfoMap rpcInfoMap;
    GigStreamRpcInfoVec rpcInfoVec(10);
    for (size_t i = 0; i < 10; ++i) {
        rpcInfoVec[i].sendStatus = GigStreamRpcStatus::GSRS_EOF;
        rpcInfoVec[i].receiveStatus = GigStreamRpcStatus::GSRS_CANCELLED;
    }
    rpcInfoVec[2].sendStatus = GigStreamRpcStatus::GSRS_EOF;
    rpcInfoVec[2].receiveStatus = GigStreamRpcStatus::GSRS_EOF;
    rpcInfoMap.emplace(std::make_pair("source_biz", "test_biz"), rpcInfoVec);
    return rpcInfoMap;
}

TEST_F(SqlResultFormatterTest, testFormatFullJson) {
    QrsSessionSqlResult sqlResult;
    sqlResult.formatType = QrsSessionSqlResult::SQL_RF_FULL_JSON;
    sqlResult.sqlQuery = "select a from b";
    sqlResult.table = createSimpleTable();
    sqlResult.searchInfoLevel = QrsSessionSqlResult::SearchInfoLevel::SQL_SIL_FULL;
    SqlSearchInfo sqlSearchInfo;
    auto scanInfo = sqlSearchInfo.add_scaninfos();
    scanInfo->set_tablename("table_a");
    scanInfo->set_isleader(true);
    auto modifyInfo = sqlSearchInfo.add_tablemodifyinfos();
    modifyInfo->set_tablename("table_a");
    modifyInfo->set_maxcheckpoint(123);
    SqlAccessLog accessLog;
    accessLog._info.searchInfo = sqlSearchInfo;
    accessLog._info.rpcInfoMap = constructLackRpcInfoMap();

    SqlAccessLogFormatHelper accessLogHelper(accessLog);
    SqlResultFormatter::formatFullJson(sqlResult, &accessLogHelper);
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
    "search_info": {
        "scanInfos": [
            {
                "isLeader": true,
                "tableName": "table_a"
            }
        ],
        "tableModifyInfos": [
            {
                "maxCheckpoint": 123,
                "tableName": "table_a"
            }
        ]
    },
    "table_build_watermark": {"table_a":123},
    "table_leader_info": {},
    "rpc_info": "%%placeholder%%",
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
    "has_soft_failure":true,
    "total_time": 0.0,
    "navi_graph":"",
    "trace": []
})json";
    StringUtil::replaceAll(
        expectStr, "%%placeholder%%", StringUtil::toString(accessLog._info.rpcInfoMap));
    ASSERT_NO_FATAL_FAILURE(JsonTestUtil::checkJsonStringEqual(expectStr, sqlResult.resultStr));
}
TEST_F(SqlResultFormatterTest, testFormatJson) {
    QrsSessionSqlResult sqlResult;
    sqlResult.formatType = QrsSessionSqlResult::SQL_RF_JSON;
    sqlResult.sqlQuery = "select a from b";
    sqlResult.table = createSimpleTable();
    sqlResult.searchInfoLevel = QrsSessionSqlResult::SearchInfoLevel::SQL_SIL_FULL;
    SqlSearchInfo sqlSearchInfo;

    SqlAccessLog accessLog;
    accessLog._info.searchInfo = sqlSearchInfo;
    accessLog._info.rpcInfoMap = constructLackRpcInfoMap();

    SqlAccessLogFormatHelper accessLogHelper(accessLog);
    SqlResultFormatter::formatJson(sqlResult, &accessLogHelper);
    string expectStr = R"json({
    "error_info": "{\n\"errorCode\": 0,\n\"errorMsg\": ERROR_NONE\n}\n",
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
    "rpc_info": "%%placeholder%%",
    "sql_query": "select a from b",
    "sql_result": "{\"data\":[[\"a\"]],\"column_name\":[\"value\"],\"column_type\":[\"multi_char\"]}",
    "total_time": 0.0,
    "table_build_watermark": {},
    "table_leader_info": {},
    "covered_percent":0.1,
    "has_soft_failure":true,
    "navi_graph":"",
    "trace": []
})json";
    StringUtil::replaceAll(
        expectStr, "%%placeholder%%", StringUtil::toString(accessLog._info.rpcInfoMap));
    ASSERT_NO_FATAL_FAILURE(JsonTestUtil::checkJsonStringEqual(expectStr, sqlResult.resultStr));
}
TEST_F(SqlResultFormatterTest, testFormatString) {
    QrsSessionSqlResult sqlResult;
    sqlResult.formatType = QrsSessionSqlResult::SQL_RF_STRING;
    sqlResult.sqlQuery = "select a from b";
    sqlResult.table = createSimpleTable();
    sqlResult.searchInfoLevel = QrsSessionSqlResult::SearchInfoLevel::SQL_SIL_FULL;
    SqlAccessLog accessLog;
    accessLog._info.rpcInfoMap = constructLackRpcInfoMap();

    SqlAccessLogFormatHelper accessLogHelper(accessLog);
    SqlResultFormatter::formatString(sqlResult, &accessLogHelper);
    string expectStr = R"string(USE_TIME: 0ms, ROW_COUNT: 0, HAS_SOFT_FAILURE: 1, COVERAGE: 0.1

------------------------------- TABLE INFO ---------------------------
total:1, rows:[0, 1), cols:1
          value (multi_char) |
                           a |

------------------------------- SEARCH INFO ---------------------------


------------------------------- RPC INFO ---------------------------
%%placeholder%%

------------------------------- PLAN INFO ---------------------------
SQL QUERY:
select a from b
IQUAN_RESULT:
{"error_code":0,"error_message":"","result":{"rel_plan_version":"","rel_plan":[],"exec_params":{}}}

------------------------------- TRACE INFO ---------------------------
)string";
    StringUtil::replaceAll(
        expectStr, "%%placeholder%%", StringUtil::toString(accessLog._info.rpcInfoMap));
    ASSERT_EQ(expectStr, sqlResult.resultStr);
}

TEST_F(SqlResultFormatterTest, testFormatTable) {
    QrsSessionSqlResult sqlResult;
    sqlResult.formatType = QrsSessionSqlResult::SQL_RF_TABLE;
    sqlResult.table = createSimpleTable();

    SqlAccessLog accessLog;
    SqlAccessLogFormatHelper accessLogHelper(accessLog);
    SqlResultFormatter::formatTable(sqlResult, &accessLogHelper, &_pool);

    SqlSearcherResponse response;
    ASSERT_TRUE(response.ParseFromString(sqlResult.resultStr));
    autil::mem_pool::PoolPtr pool(new autil::mem_pool::PoolAsan());
    TablePtr table(new Table(pool));
    table->deserializeFromString(response.tabledata(), pool.get());
    TableTestUtil::checkOutputColumn<std::string>(table, "value", {"a"});
}

} // namespace sql
