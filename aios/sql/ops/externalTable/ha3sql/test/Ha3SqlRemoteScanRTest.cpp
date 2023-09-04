#include "sql/ops/externalTable/ha3sql/Ha3SqlRemoteScanR.h"

#include <algorithm>
#include <iosfwd>
#include <map>
#include <memory>
#include <stdint.h>
#include <string.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/TimeoutTerminator.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/md5.h"
#include "autil/result/Errors.h"
#include "autil/result/ForwardList.h"
#include "autil/result/Result.h"
#include "iquan/common/Common.h"
#include "navi/common.h"
#include "navi/engine/test/MockAsyncPipe.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/log/test/NaviLoggerProviderTestUtil.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/common/FieldMeta.h"
#include "sql/common/TableMeta.h"
#include "sql/common/common.h"
#include "sql/config/ExternalTableConfigR.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/calc/CalcInitParamR.h"
#include "sql/ops/condition/OutputFieldsVisitor.h"
#include "sql/ops/externalTable/GigQuerySessionCallbackCtxR.h"
#include "sql/ops/externalTable/ha3sql/Ha3SqlConditionVisitor.h"
#include "sql/ops/scan/ScanInitParamR.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "sql/resource/TimeoutTerminatorR.h"
#include "suez/sdk/IndexProviderR.h"
#include "table/Row.h"
#include "unittest/unittest.h"

using namespace std;
using namespace navi;
using namespace testing;
using namespace table;
using namespace autil::legacy;
using namespace autil::result;

namespace sql {

class Ha3SqlRemoteScanRTest : public TESTBASE {
public:
    Ha3SqlRemoteScanRTest() {}
    ~Ha3SqlRemoteScanRTest() {}

public:
    void setUp() override;

private:
    NaviResourceHelper _naviRes;
    navi::GraphMemoryPoolR *_graphMemoryPoolR = nullptr;
    TimeoutTerminatorR *_timeoutTerminatorR = nullptr;
};

void Ha3SqlRemoteScanRTest::setUp() {
    _graphMemoryPoolR = _naviRes.getOrCreateRes<GraphMemoryPoolR>();
    ASSERT_TRUE(_graphMemoryPoolR);
    _timeoutTerminatorR = _naviRes.getOrCreateRes<TimeoutTerminatorR>();
    ASSERT_TRUE(_timeoutTerminatorR);
    ASSERT_TRUE(_naviRes.addExternalRes(std::make_shared<suez::IndexProviderR>()));
}

TEST_F(Ha3SqlRemoteScanRTest, testInitOutputVisitor_WithExprJson) {
    // SELECT avg_value as data, tags['host'] as taghost, tags['app'], watermark + 1 as wm ...
    CalcInitParamR calcR;
    ScanInitParamR param;
    param.calcInitParamR = &calcR;
    calcR.outputFieldsOrigin = {
        "$data",
        "$taghost",
        "$tags['app']",
        "$wm",
    };
    calcR.outputExprsJson = R"json({
    "$data": "$avg_value",
    "$taghost": {
        "op": "ITEM",
        "params": [
            "$tags",
            "host"
        ],
        "type": "OTHER"
    },
    "$tags['app']": {
        "op": "ITEM",
        "params": [
            "$tags",
            "app"
        ],
        "type": "OTHER"
    },
    "$wm": {
        "op": "+",
        "params": [
            "$watermark",
            1
        ],
        "type": "OTHER"
    }
})json";
    auto pipe = std::make_shared<MockAsyncPipe>();
    Ha3SqlRemoteScanR scan;
    scan._scanInitParamR = &param;
    scan._graphMemoryPoolR = _graphMemoryPoolR;
    ASSERT_TRUE(scan.initOutputFieldsVisitor());
    ASSERT_NE(nullptr, scan._outputFieldsVisitor);

    // --> SELECT avg_value, watermark, tags['host'], tags['app'] FROM remote.xxx
    ASSERT_THAT(scan._outputFieldsVisitor->getUsedFieldsColumnSet(),
                UnorderedElementsAre("$watermark", "$avg_value"));

    ASSERT_THAT(scan._outputFieldsVisitor->getUsedFieldsItemSet(),
                UnorderedElementsAre(Pair("$tags", "host"), Pair("$tags", "app")));
}

TEST_F(Ha3SqlRemoteScanRTest, testInitOutputFieldsVisitor_WithoutExprJson) {
    CalcInitParamR calcR;
    ScanInitParamR param;
    param.calcInitParamR = &calcR;
    calcR.outputFieldsOrigin = {
        "$cat_id",
        "$category_name",
    };
    calcR.outputExprsJson = R"json({})json";
    auto pipe = std::make_shared<MockAsyncPipe>();
    Ha3SqlRemoteScanR scan;
    scan._scanInitParamR = &param;
    scan._asyncPipe = pipe;
    scan._graphMemoryPoolR = _graphMemoryPoolR;
    ASSERT_TRUE(scan.initOutputFieldsVisitor());
    ASSERT_NE(nullptr, scan._outputFieldsVisitor);
    ASSERT_THAT(scan._outputFieldsVisitor->getUsedFieldsColumnSet(),
                UnorderedElementsAre("$cat_id", "$category_name"));
    ASSERT_THAT(scan._outputFieldsVisitor->getUsedFieldsItemSet(), UnorderedElementsAre());
}

std::string getSignature(const std::string &queryStr, const std::string &authKey) {
    vector<string> clauses = autil::StringUtil::split(queryStr, SQL_CLAUSE_SPLIT, true);
    if (clauses.empty()) {
        return "";
    }
    for (auto &clause : clauses) {
        autil::StringUtil::trim(clause);
    }
    std::string kvpairClause;
    std::string queryClause;
    for (const auto &clause : clauses) {
        if (clause.find(SQL_KVPAIR_CLAUSE) == 0) {
            kvpairClause = clause.substr(strlen(SQL_KVPAIR_CLAUSE));
        } else if (clause.find(SQL_QUERY_CLAUSE) == 0) {
            queryClause = clause.substr(strlen(SQL_QUERY_CLAUSE));
        }
    }
    autil::legacy::Md5Stream stream;
    stream.Put((const uint8_t *)queryClause.c_str(), queryClause.size());
    stream.Put((const uint8_t *)kvpairClause.c_str(), kvpairClause.size());
    stream.Put((const uint8_t *)authKey.c_str(), authKey.size());
    return stream.GetMd5String();
}

TEST_F(Ha3SqlRemoteScanRTest, testGenQueryString) {
    ScanInitParamR param;
    FieldMeta meta;
    meta.indexName = "ccc";
    meta.indexType = "primary_key";
    param.tableMeta.fieldsMeta = {meta};
    auto pipe = std::make_shared<MockAsyncPipe>();
    Ha3SqlRemoteScanR scan;
    scan._scanInitParamR = &param;
    scan._graphMemoryPoolR = _graphMemoryPoolR;
    scan._outputFieldsVisitor.reset(new OutputFieldsVisitor());
    scan._conditionVisitor.reset(new Ha3SqlConditionVisitor(_graphMemoryPoolR->getPool()));
    scan._extraQuery.reset(new StreamQuery());
    scan._extraQuery->primaryKeys = {"1", "2", "3"};
    Ha3SqlRemoteScanR::QueryGenerateParam queryParam;
    queryParam.dbName = "db";
    queryParam.tableName = "table";
    queryParam.authToken = "token";
    queryParam.authKey = "key";
    queryParam.enableAuth = true;
    std::string query;
    ASSERT_TRUE(scan.genQueryString(queryParam, query));
    ASSERT_THAT(query, HasSubstr("WHERE 1=1 AND contain(`ccc`, ?)"));
    ASSERT_THAT(query, HasSubstr(R"str(dynamic_params:[["1|2|3"]])str"));
    auto sign = getSignature(query, queryParam.authKey);
    ASSERT_THAT(query, HasSubstr("authToken=token&&authSignature=" + sign));
}

TEST_F(Ha3SqlRemoteScanRTest, testPrepareQueryGenerateParam) {
    const std::string exampleConfig = R"json({
    "gig_config": {
        "subscribe": {
        },
        "flow_control": {
        }
    },
    "service_config": {
        "a.external": {
            "type": "HA3SQL",
            "engine_version": "3.12.0",
            "misc_config": {
                "auth_token": "token",
                "auth_key": "key",
                "auth_enabled": true
            }
        }
    },
    "table_config": {
        "table_a": {
            "schema_file": "",
            "database_name": "share1",
            "table_name": "b",
            "service_name": "a.external"
        }
    }
    })json";

    _naviRes.config(ExternalTableConfigR::RESOURCE_ID, exampleConfig);
    auto *config = _naviRes.getOrCreateRes<ExternalTableConfigR>();
    ASSERT_TRUE(config);

    ScanInitParamR param;
    param.tableName = "table_a";
    Ha3SqlRemoteScanR scan;
    scan._scanInitParamR = &param;
    scan._externalTableConfigR = config;
    scan._graphMemoryPoolR = _graphMemoryPoolR;
    scan._timeoutTerminatorR = _timeoutTerminatorR;
    ASSERT_TRUE(scan.initWithExternalTableConfig());
    Ha3SqlRemoteScanR::QueryGenerateParam queryParam;
    ASSERT_TRUE(scan.prepareQueryGenerateParam(queryParam));
    ASSERT_EQ("a.external", queryParam.serviceName);
    ASSERT_EQ("share1", queryParam.dbName);
    ASSERT_EQ("b", queryParam.tableName);
    ASSERT_EQ("token", queryParam.authToken);
    ASSERT_EQ("key", queryParam.authKey);
    ASSERT_TRUE(queryParam.enableAuth);
}

TEST_F(Ha3SqlRemoteScanRTest, testInitWithExternalTableConfig_error) {
    {
        ScanInitParamR param;
        ExternalTableConfigR config;
        Ha3SqlRemoteScanR scan;
        scan._scanInitParamR = &param;
        scan._externalTableConfigR = &config;
        NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(scan.initWithExternalTableConfig());
        ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
            1, "table[] not configured in external table config", provider));
    }
    {
        ScanInitParamR param;
        param.tableName = "table";
        ExternalTableConfigR config;
        config.tableConfigMap.emplace("table", TableConfig {});
        Ha3SqlRemoteScanR scan;
        scan._scanInitParamR = &param;
        scan._externalTableConfigR = &config;
        NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(scan.initWithExternalTableConfig());
        ASSERT_NO_FATAL_FAILURE(
            NaviLoggerProviderTestUtil::checkTraceCount(1, "invalid table config", provider));
    }
    {
        ScanInitParamR param;
        param.tableName = "table";
        ExternalTableConfigR config;
        TableConfig table;
        table.serviceName = "s";
        table.dbName = SQL_DEFAULT_EXTERNAL_DATABASE_NAME;
        table.tableName = "t";
        config.tableConfigMap.emplace("table", table);
        Ha3SqlRemoteScanR scan;
        scan._scanInitParamR = &param;
        scan._externalTableConfigR = &config;
        NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(scan.initWithExternalTableConfig());
        ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
            1, "accessing external table recursively not allowed", provider));
    }
    {
        ScanInitParamR param;
        param.tableName = "table";
        ExternalTableConfigR config;
        TableConfig table;
        table.serviceName = "service";
        table.dbName = "db";
        table.tableName = "table";
        config.tableConfigMap.emplace("table", table);
        Ha3SqlRemoteScanR scan;
        scan._scanInitParamR = &param;
        scan._externalTableConfigR = &config;
        NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(scan.initWithExternalTableConfig());
        ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
            1, "not found in service configs", provider));
    }
    {
        ScanInitParamR param;
        param.tableName = "table";
        ExternalTableConfigR config;
        TableConfig table;
        table.serviceName = "service";
        table.dbName = "db";
        table.tableName = "table";

        ServiceConfig service;
        service.type = "aaaa";
        config.tableConfigMap.emplace("table", std::move(table));
        config.serviceConfigMap.emplace("service", std::move(service));
        Ha3SqlRemoteScanR scan;
        scan._scanInitParamR = &param;
        scan._externalTableConfigR = &config;
        NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(scan.initWithExternalTableConfig());
        ASSERT_NO_FATAL_FAILURE(
            NaviLoggerProviderTestUtil::checkTraceCount(1, "invalid service type", provider));
    }
    {
        ScanInitParamR param;
        param.tableName = "table";
        ExternalTableConfigR config;
        TableConfig table;
        table.serviceName = "service";
        table.dbName = "db";
        table.tableName = "table";

        ServiceConfig service;
        service.type = EXTERNAL_TABLE_TYPE_HA3SQL;
        config.tableConfigMap.emplace("table", std::move(table));
        config.serviceConfigMap.emplace("service", std::move(service));
        Ha3SqlRemoteScanR scan;
        scan._scanInitParamR = &param;
        scan._externalTableConfigR = &config;
        NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(scan.initWithExternalTableConfig());
        ASSERT_NO_FATAL_FAILURE(
            NaviLoggerProviderTestUtil::checkTraceCount(1, "cast misc config failed", provider));
    }
}

TEST_F(Ha3SqlRemoteScanRTest, testPrepareQueryGenerateParam_error) {
    const std::string configStr = R"json([{
        "name": "external_table_config_r",
        "config": {
            "service_config": {
                "biz_order_seller_online.external": {
                    "type": "HA3SQL",
                    "engine_version": "3.12.0",
                    "timeout_threshold_ms": 20,
                    "misc_config": {
                        "auth_token": "xxxx",
                        "auth_key": "xxxx",
                        "auth_enabled": false
                    }
                }
            },
            "table_config": {
                "table": {
                    "service_name": "biz_order_seller_online.external",
                    "database_name": "db",
                    "schema_file": "",
                    "table_name": "table"
                }
            }
        }
    }])json";
    _naviRes.config(configStr);
    auto externalConfR = _naviRes.getOrCreateRes<ExternalTableConfigR>();
    ASSERT_TRUE(externalConfR);
    ScanInitParamR param;
    param.tableName = "table";
    Ha3SqlRemoteScanR scan;
    scan._scanInitParamR = &param;
    scan._externalTableConfigR = externalConfR;
    scan._timeoutTerminatorR = _timeoutTerminatorR;
    // NaviLoggerProvider provider("ERROR");
    ASSERT_TRUE(scan.initWithExternalTableConfig());

    auto *newTerminator = new autil::TimeoutTerminator(10 * 1000); // 10ms < 20ms
    std::swap(scan._timeoutTerminatorR->_timeoutTerminator, newTerminator);
    delete newTerminator;
    Ha3SqlRemoteScanR::QueryGenerateParam queryParam;
    ASSERT_FALSE(scan.prepareQueryGenerateParam(queryParam));
}

TEST_F(Ha3SqlRemoteScanRTest, testDoBatchScan_WithDegraded) {
    _naviRes.kernelConfig(R"json({
        "table_name": "table",
        "table_type": "kv",
        "output_fields": []
    })json");
    auto scanInitParamR = _naviRes.getOrCreateRes<ScanInitParamR>();
    ASSERT_TRUE(scanInitParamR);
    TableConfig tableConfig;
    auto ctx = _naviRes.getOrCreateRes<GigQuerySessionCallbackCtxR>();
    ASSERT_TRUE(ctx);
    Ha3SqlRemoteScanR scan;
    scan._scanInitParamR = scanInitParamR;
    scan._tableConfig = &tableConfig;
    scan._ctx = ctx;
    scan._graphMemoryPoolR = _graphMemoryPoolR;
    {
        tableConfig.allowDegradedAccess = true; // key
        scan._ctx->_result = autil::result::RuntimeError::make("aaa bbb ccc");

        TablePtr table;
        bool eof = false;
        NaviLoggerProvider provider("ERROR");
        ASSERT_TRUE(scan.doBatchScan(table, eof));
        ASSERT_NO_FATAL_FAILURE(
            NaviLoggerProviderTestUtil::checkTraceCount(1, "get result failed", provider));

        ASSERT_TRUE(eof);
        ASSERT_NE(nullptr, table);
        ASSERT_LT(0, scan._scanInitParamR->scanInfo.degradeddocscount());
    }
    {
        tableConfig.allowDegradedAccess = false; // key
        scan._ctx->_result = autil::result::RuntimeError::make("aaa bbb ccc");

        TablePtr table;
        bool eof = false;
        NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(scan.doBatchScan(table, eof));
        ASSERT_NO_FATAL_FAILURE(
            NaviLoggerProviderTestUtil::checkTraceCount(1, "get result failed", provider));
    }
}

} // namespace sql
