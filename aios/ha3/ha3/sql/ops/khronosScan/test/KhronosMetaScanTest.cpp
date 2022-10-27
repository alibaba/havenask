#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/khronosScan/KhronosCommon.h>
#include <ha3/sql/ops/khronosScan/KhronosScanKernel.h>
#include <ha3/sql/ops/khronosScan/KhronosMetaScan.h>
#include <ha3/sql/ops/khronosScan/test/KhronosTestTableBuilder.h>
#include <ha3/sql/ops/condition/ConditionParser.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/turing/ops/SqlSessionResource.h>
#include <ha3/search/IndexPartitionReaderUtil.h>
#include <autil/DefaultHashFunction.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <navi/resource/MemoryPoolResource.h>
#include <matchdoc/MatchDocAllocator.h>

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace testing;
using namespace suez::turing;
using autil::mem_pool::Pool;
using namespace navi;
using namespace khronos::search;
using namespace khronos::indexlib_plugin;

BEGIN_HA3_NAMESPACE(sql);

class KhronosMetaScanTest : public OpTestBase {
public:
    KhronosMetaScanTest()
    {
        _tableName = "simple_table";
    }
    void setUp();
    void tearDown();
public:
    void prepareIndex() override;
private:
    ScanInitParam createBaseScanInitParam();
    ScanInitParam createAllSuccessScanTagvInitParam();
    ScanInitParam createAllSuccessScanTagkInitParam();
    void testParseQueryParamsFromConditionSuccess(
            const string &conditionJson,
            KhronosMetaScan &scan)
    {
        ScanInitParam param = createBaseScanInitParam();
        scan._conditionJson = conditionJson;
        scan._indexInfos = param.indexInfos;
        // check
        ASSERT_TRUE(scan.parseQueryParamsFromCondition());
    }
    void testParseQueryParamsFromConditionFail(
            KhronosMetaScan::KhronosScanType scanType,
            const string &conditionJson,
            const string &errorHint)
    {
        KhronosMetaScan scan;
        ScanInitParam param = createBaseScanInitParam();
        scan._scanType = scanType;
        scan._conditionJson = conditionJson;
        scan._indexInfos = param.indexInfos;
        // check
        navi::NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(scan.parseQueryParamsFromCondition());
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, errorHint, traces);
    }

private:
    MemoryPoolResource _memoryPoolResource;
    IndexInfoMapType _indexInfos;
    KhronosTestTableBuilder _tableBuilder;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(searcher, KhronosMetaScanTest);

void KhronosMetaScanTest::setUp() {
    _needBuildIndex = true;
    _needExprResource = false;

    string indexInfosStr = R"json({
    "$tagv" : {
        "name" : "tagv",
        "type" : "khronos_tag_value"
    },
    "$metric" : {
        "name" : "metric",
        "type" : "khronos_metric"
    },
    "$ts" : {
        "name" : "ts",
        "type" : "khronos_timestamp"
    },
    "$tagk" : {
        "name" : "tagk",
        "type" : "khronos_tag_key"
    }
})json";
    FromJsonString(_indexInfos, indexInfosStr);
}

void KhronosMetaScanTest::tearDown() {
    clearResource();
    _indexApp.reset();
}

void KhronosMetaScanTest::prepareIndex() {
    ASSERT_TRUE(_tableBuilder.build(_testPath));
    _indexApp = _tableBuilder._indexApp;
}

ScanInitParam KhronosMetaScanTest::createBaseScanInitParam() {
    ScanInitParam param;
    param.catalogName = _tableName;
    param.tableType = KHRONOS_META_TABLE_TYPE;
    param.bizResource = _sqlBizResource.get();
    param.queryResource = _sqlQueryResource.get();
    param.indexInfos = _indexInfos;
    param.memoryPoolResource = &_memoryPoolResource;
    return param;
}

ScanInitParam KhronosMetaScanTest::createAllSuccessScanTagvInitParam() {
    ScanInitParam param = createBaseScanInitParam();
    param.outputFields = {"$tagv"};
    param.conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "<",
            "params" :
            [
                1546272001,
                "$ts"
            ]
        },
        {
            "op" : "=",
            "params" :
            [
                "$tagk",
                "host"
            ]
        },
        {
            "op" : "=",
            "params" :
            [
                "$metric",
                "cpu"
            ]
        },
        {
            "op" : "LIKE",
            "params" :
            [
                "$tagv",
                "10.10*"
            ]
        }
    ]
})json";
    return param;
}

ScanInitParam KhronosMetaScanTest::createAllSuccessScanTagkInitParam() {
    ScanInitParam param = createBaseScanInitParam();
    param.outputFields = {"$tagk"};
    param.conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "=",
            "params" :
            [
                "$metric",
                "cpu"
            ]
        }
    ]
})json";
    return param;
}

TEST_F(KhronosMetaScanTest, testAllSuccessScanTagv) {
    ScanInitParam param = createAllSuccessScanTagvInitParam();
    KhronosMetaScan scan;
    ASSERT_TRUE(scan.init(param));
    ASSERT_EQ(KhronosMetaScan::KST_TAGV, scan._scanType);
    ASSERT_EQ(1546272002, scan._tsRange.begin);
    ASSERT_EQ(khronos::KHR_TS_TYPE(TimeRange::maxTime), scan._tsRange.end);
    ASSERT_EQ(SearchInterface::METRIC_PLAIN_TEXT, scan._metric.mMetricMatchType);
    ASSERT_EQ("cpu", scan._metric.mMetricPattern);
    ASSERT_EQ("host", scan._tagk);
    ASSERT_EQ("10.10", scan._tagv);

    TablePtr outputTable;
    bool eof;
    ASSERT_TRUE(scan.doBatchScan(outputTable, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_EQ(1, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "tagv", {"10.10.10.1", "10.10.10.2"}));
}

TEST_F(KhronosMetaScanTest, testAllSuccessScanTagv_WithLimit) {
    {
        ScanInitParam param = createAllSuccessScanTagvInitParam();
        KhronosMetaScan scan;
        ASSERT_TRUE(scan.init(param));
        scan._limit = 0;

        TablePtr outputTable;
        bool eof;
        ASSERT_TRUE(scan.doBatchScan(outputTable, eof));
        ASSERT_TRUE(eof);
        ASSERT_EQ(0, outputTable->getRowCount());
        ASSERT_EQ(1, outputTable->getColumnCount());
    }
    {
        ScanInitParam param = createAllSuccessScanTagvInitParam();
        KhronosMetaScan scan;
        ASSERT_TRUE(scan.init(param));
        scan._limit = 1;

        TablePtr outputTable;
        bool eof;
        ASSERT_TRUE(scan.doBatchScan(outputTable, eof));
        ASSERT_TRUE(eof);
        ASSERT_EQ(1, outputTable->getRowCount());
        ASSERT_EQ(1, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "tagv", {"10.10.10.1"}));
    }
}

TEST_F(KhronosMetaScanTest, testAllSuccessScanTagk_WithUselessLimit) {
    {
        ScanInitParam param = createAllSuccessScanTagkInitParam();
        KhronosMetaScan scan;
        ASSERT_TRUE(scan.init(param));
        scan._limit = 0; // no use

        TablePtr outputTable;
        bool eof;
        ASSERT_TRUE(scan.doBatchScan(outputTable, eof));
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, outputTable->getRowCount());
        ASSERT_EQ(1, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "tagk", {"app", "host"}));
    }
    {
        ScanInitParam param = createAllSuccessScanTagkInitParam();
        KhronosMetaScan scan;
        ASSERT_TRUE(scan.init(param));
        scan._limit = 1; // no use

        TablePtr outputTable;
        bool eof;
        ASSERT_TRUE(scan.doBatchScan(outputTable, eof));
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, outputTable->getRowCount());
        ASSERT_EQ(1, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "tagk", {"app", "host"}));
    }
}

TEST_F(KhronosMetaScanTest, testInit_InvalidOutputFields) {
    {
        ScanInitParam param = createBaseScanInitParam();
        param.outputFields = {};

        navi::NaviLoggerProvider provider("ERROR");
        KhronosMetaScan scan;
        ASSERT_FALSE(scan.init(param));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "output fields size is [0], must be 1", traces);
    }
    {
        ScanInitParam param = createBaseScanInitParam();
        param.outputFields = {"aa", "bb"};

        navi::NaviLoggerProvider provider("ERROR");
        KhronosMetaScan scan;
        ASSERT_FALSE(scan.init(param));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "output fields size is [2], must be 1", traces);
    }
}

TEST_F(KhronosMetaScanTest, testCreateSingleColumnTable_Limit) {
    KhronosMetaScan scan;
    scan._memoryPoolResource = &_memoryPoolResource;
    scan._pool = &_pool;
    scan._kernelPool = _memoryPoolResource.getPool();
    {
        // limit = 4
        TablePtr outputTable;
        ASSERT_TRUE(scan.createSingleColumnTable(
                        {"a", "b", "c", "d", "e"}, "id", 4, outputTable));
        ASSERT_EQ(4, outputTable->getRowCount());
        ASSERT_EQ(1, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "id", {"a", "b", "c", "d"}));
    }
    {
        // limit = 5
        TablePtr outputTable;
        ASSERT_TRUE(scan.createSingleColumnTable(
                        {"a", "b", "c", "d", "e"}, "id", 5, outputTable));
        ASSERT_EQ(5, outputTable->getRowCount());
        ASSERT_EQ(1, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "id", {"a", "b", "c", "d", "e"}));
    }
    {
        // limit = 6
        TablePtr outputTable;
        ASSERT_TRUE(scan.createSingleColumnTable(
                        {"a", "b", "c", "d", "e"}, "id", 6, outputTable));
        ASSERT_EQ(5, outputTable->getRowCount());
        ASSERT_EQ(1, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "id", {"a", "b", "c", "d", "e"}));
    }
}

TEST_F(KhronosMetaScanTest, testInit_InvalidScanType) {
    ScanInitParam param = createBaseScanInitParam();
    param.outputFields = {"unknown"};

    navi::NaviLoggerProvider provider("ERROR");
    KhronosMetaScan scan;
    ASSERT_FALSE(scan.init(param));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "fill scan type failed", traces);
}

TEST_F(KhronosMetaScanTest, testInit_InvalidVisitAndParseCondition) {
    ScanInitParam param = createBaseScanInitParam();
    param.outputFields = {"$tagv"};
    param.conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : ">",
            "params" :
            [
                "$tagk",
                "host"
            ]
        }
    ]
})json";

    navi::NaviLoggerProvider provider("ERROR");
    KhronosMetaScan scan;
    ASSERT_FALSE(scan.init(param));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "visit and parse condition failed", traces);
}

TEST_F(KhronosMetaScanTest, testInit_InvalidValidateAndFetchConditionParams) {
    ScanInitParam param = createBaseScanInitParam();
    param.outputFields = {"$tagv"};
    param.conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "=",
            "params" :
            [
                "$metric",
                "sys.cpu"
            ]
        },
        {
            "op" : "=",
            "params" :
            [
                "$metric",
                "sys.cpu2"
            ]
        }
    ]
})json";

    navi::NaviLoggerProvider provider("ERROR");
    KhronosMetaScan scan;
    ASSERT_FALSE(scan.init(param));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "parse query params from condition failed", traces);
}

TEST_F(KhronosMetaScanTest, testInit_InvalidSqlQueryResource) {
    ScanInitParam param = createBaseScanInitParam();
    param.outputFields = {"$metric"};
    param.conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
    ]
})json";
    param.queryResource = nullptr;

    navi::NaviLoggerProvider provider("ERROR");
    KhronosMetaScan scan;
    ASSERT_FALSE(scan.init(param));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "get sql query resource failed.", traces);
}

TEST_F(KhronosMetaScanTest, testInit_InvalidKhronosTableReader) {
    ScanInitParam param = createBaseScanInitParam();
    param.catalogName = "invalidTable";
    param.outputFields = {"$metric"};
    param.conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
    ]
})json";

    navi::NaviLoggerProvider provider("ERROR");
    KhronosMetaScan scan;
    ASSERT_FALSE(scan.init(param));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1,
                      "get khronos table reader failed, "
                      "catalogName: [invalidTable], dbName: [], tableName[].",
                      traces);
}

TEST_F(KhronosMetaScanTest, testParseQueryParamsFromCondition_Success_KST_METRIC)
{
    {
        KhronosMetaScan scan;
        scan._scanType = KhronosMetaScan::KST_METRIC;
        string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "LIKE",
            "params" :
            [
                "$metric",
                "cpu*"
            ]
        }
    ]
})json";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionSuccess(conditionJson, scan));
        ASSERT_EQ(SearchInterface::METRIC_PREFIX, scan._metric.mMetricMatchType);
        ASSERT_EQ("cpu", scan._metric.mMetricPattern);
        ASSERT_EQ("", scan._tagk);
        ASSERT_EQ("", scan._tagv);
    }

    {
        KhronosMetaScan scan;
        scan._scanType = KhronosMetaScan::KST_METRIC;
        string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
    ]
})json";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionSuccess(conditionJson, scan));
        ASSERT_EQ(SearchInterface::METRIC_PREFIX, scan._metric.mMetricMatchType);
        ASSERT_EQ("", scan._metric.mMetricPattern);
        ASSERT_EQ("", scan._tagk);
        ASSERT_EQ("", scan._tagv);
    }

    {
        KhronosMetaScan scan;
        scan._scanType = KhronosMetaScan::KST_METRIC;
        string conditionJson = R"json()json";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionSuccess(conditionJson, scan));
        ASSERT_EQ(SearchInterface::METRIC_PREFIX, scan._metric.mMetricMatchType);
        ASSERT_EQ("", scan._metric.mMetricPattern);
        ASSERT_EQ("", scan._tagk);
        ASSERT_EQ("", scan._tagv);
    }
}

TEST_F(KhronosMetaScanTest, testParseQueryParamsFromCondition_Success_KST_TAGK)
{
    {
        KhronosMetaScan scan;
        scan._scanType = KhronosMetaScan::KST_TAGK;
        string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "=",
            "params" :
            [
                "$metric",
                "cpu"
            ]
        }
    ]
})json";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionSuccess(conditionJson, scan));
        ASSERT_EQ(SearchInterface::METRIC_PLAIN_TEXT, scan._metric.mMetricMatchType);
        ASSERT_EQ("cpu", scan._metric.mMetricPattern);
        ASSERT_EQ("", scan._tagk);
        ASSERT_EQ("", scan._tagv);
    }

    {
        KhronosMetaScan scan;
        scan._scanType = KhronosMetaScan::KST_TAGK;
        string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
    ]
})json";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionSuccess(conditionJson, scan));
        ASSERT_EQ(SearchInterface::METRIC_PLAIN_TEXT, scan._metric.mMetricMatchType);
        ASSERT_EQ("", scan._metric.mMetricPattern);
        ASSERT_EQ("", scan._tagk);
        ASSERT_EQ("", scan._tagv);
    }
}

TEST_F(KhronosMetaScanTest, testParseQueryParamsFromCondition_Success_KST_TAGV)
{
    {
        KhronosMetaScan scan;
        scan._scanType = KhronosMetaScan::KST_TAGV;
        string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "=",
            "params" :
            [
                "$metric",
                "cpu"
            ]
        },
        {
            "op" : "=",
            "params" :
            [
                "$tagk",
                "host"
            ]
        }
    ]
})json";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionSuccess(conditionJson, scan));
        ASSERT_EQ(SearchInterface::METRIC_PLAIN_TEXT, scan._metric.mMetricMatchType);
        ASSERT_EQ("cpu", scan._metric.mMetricPattern);
        ASSERT_EQ("host", scan._tagk);
        ASSERT_EQ("", scan._tagv);
    }

    {
        KhronosMetaScan scan;
        scan._scanType = KhronosMetaScan::KST_TAGV;
        string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "=",
            "params" :
            [
                "$tagk",
                "host"
            ]
        },
        {
            "op" : "LIKE",
            "params" :
            [
                "$tagv",
                "10.10*"
            ]
        }
    ]
})json";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionSuccess(conditionJson, scan));
        ASSERT_EQ(SearchInterface::METRIC_PLAIN_TEXT, scan._metric.mMetricMatchType);
        ASSERT_EQ("", scan._metric.mMetricPattern);
        ASSERT_EQ("host", scan._tagk);
        ASSERT_EQ("10.10", scan._tagv);
    }
}

TEST_F(KhronosMetaScanTest, testParseQueryParamsFromCondition_Error_InvalidTsRange) {
    auto scanType = KhronosMetaScan::KST_METRIC;
    {
        string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "<",
            "params" :
            [
                100,
                "$ts"
            ]
        },
        {
            "op" : ">",
            "params" :
            [
                100,
                "$ts"
            ]
        }
    ]
})json";
        string errorHint = "time stamp range is invalid";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionFail(scanType, conditionJson, errorHint));
    }
}

TEST_F(KhronosMetaScanTest, testParseQueryParamsFromCondition_Error_KST_METRIC) {
    auto scanType = KhronosMetaScan::KST_METRIC;
    {
        string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "=",
            "params" :
            [
                "$metric",
                "cpu"
            ]
        },
        {
            "op" : "=",
            "params" :
            [
                "$metric",
                "cpu"
            ]
        }
    ]
})json";
        string errorHint = "SELECT METRIC: metric condition num must less than 2";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionFail(scanType, conditionJson, errorHint));
    }

    {
        string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "=",
            "params" :
            [
                "$metric",
                "cpu"
            ]
        }
    ]
})json";
        string errorHint = "SELECT METRIC: metric condition can not be exact match";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionFail(scanType, conditionJson, errorHint));
    }

    {
        string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "=",
            "params" :
            [
                "$tagk",
                "host"
            ]
        }
    ]
})json";
        string errorHint = "SELECT METRIC: tagk condition must be empty";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionFail(scanType, conditionJson, errorHint));
    }

    {
        string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "=",
            "params" :
            [
                "$tagv",
                "10.10"
            ]
        }
    ]
})json";
        string errorHint = "SELECT METRIC: tagv condition must be empty";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionFail(scanType, conditionJson, errorHint));
    }
}

TEST_F(KhronosMetaScanTest, testParseQueryParamsFromCondition_Error_KST_TAGK) {
    auto scanType = KhronosMetaScan::KST_TAGK;
    {
        string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "=",
            "params" :
            [
                "$metric",
                "cpu"
            ]
        },
        {
            "op" : "=",
            "params" :
            [
                "$metric",
                "cpu"
            ]
        }
    ]
})json";
        string errorHint = "SELECT TAGK: metric condition num must less than 2";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionFail(scanType, conditionJson, errorHint));
    }

    {
        string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "LIKE",
            "params" :
            [
                "$metric",
                "cpu*"
            ]
        }
    ]
})json";
        string errorHint = "SELECT TAGK: metric condition must be exact match";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionFail(scanType, conditionJson, errorHint));
    }

    {
        string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "=",
            "params" :
            [
                "$tagk",
                "host"
            ]
        }
    ]
})json";
        string errorHint = "SELECT TAGK: tagk condition must be empty";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionFail(scanType, conditionJson, errorHint));
    }

    {
        string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "=",
            "params" :
            [
                "$tagv",
                "10.10"
            ]
        }
    ]
})json";
        string errorHint = "SELECT TAGK: tagv condition must be empty";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionFail(scanType, conditionJson, errorHint));
    }
}

TEST_F(KhronosMetaScanTest, testParseQueryParamsFromCondition_Error_KST_TAGV) {
    auto scanType = KhronosMetaScan::KST_TAGV;
    {
        string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "=",
            "params" :
            [
                "$metric",
                "cpu"
            ]
        },
        {
            "op" : "=",
            "params" :
            [
                "$metric",
                "cpu"
            ]
        }
    ]
})json";
        string errorHint = "SELECT TAGV: metric condition num must less than 2";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionFail(scanType, conditionJson, errorHint));
    }

    {
        string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "LIKE",
            "params" :
            [
                "$metric",
                "cpu*"
            ]
        }
    ]
})json";
        string errorHint = "SELECT TAGV: metric condition must be exact match";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionFail(scanType, conditionJson, errorHint));
    }

    {
        string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
    ]
})json";
        string errorHint = "SELECT TAGV: tagk condition num must be 1";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionFail(scanType, conditionJson, errorHint));
    }

    {
        string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "LIKE",
            "params" :
            [
                "$tagk",
                "host*"
            ]
        }
    ]
})json";
        string errorHint = "SELECT TAGV: tagk condition can not end with *";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionFail(scanType, conditionJson, errorHint));
    }

    {
        string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "=",
            "params" :
            [
                "$tagk",
                "host"
            ]
        },
        {
            "op" : "=",
            "params" :
            [
                "$tagv",
                "10.10"
            ]
        }
    ]
})json";
        string errorHint = "SELECT TAGV: tagv condition must end with *";
        ASSERT_NO_FATAL_FAILURE(
                testParseQueryParamsFromConditionFail(scanType, conditionJson, errorHint));
    }
}

END_HA3_NAMESPACE();
