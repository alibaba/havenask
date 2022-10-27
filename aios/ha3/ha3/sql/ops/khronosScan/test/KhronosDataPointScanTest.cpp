#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/khronosScan/KhronosCommon.h>
#include <ha3/sql/ops/khronosScan/KhronosScanKernel.h>
#include <ha3/sql/ops/khronosScan/KhronosDataPointScan.h>
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

class KhronosDataPointScanTest : public OpTestBase {
public:
    KhronosDataPointScanTest()
    {
        _tableName = "simple_table";
    }
    void setUp();
    void tearDown();
public:
    void prepareIndex() override;
private:
    ScanInitParam createBaseScanInitParam();
    ScanInitParam createAllSuccessScanInitParam();
private:
    MemoryPoolResource _memoryPoolResource;
    IndexInfoMapType _indexInfos;
    KhronosTestTableBuilder _tableBuilder;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(searcher, KhronosDataPointScanTest);

void KhronosDataPointScanTest::setUp() {
    _needBuildIndex = true;
    _needExprResource = false;

    string indexInfosStr = R"json({
    "$value": {
        "name": "value",
        "type": "khronos_value"
    },
    "$timestamp": {
        "name": "timestamp",
        "type": "khronos_timestamp"
    },
    "$watermark": {
        "name": "watermark",
        "type": "khronos_watermark"
    }
})json";
    FromJsonString(_indexInfos, indexInfosStr);
}

void KhronosDataPointScanTest::tearDown() {
    clearResource();
    _indexApp.reset();
}

void KhronosDataPointScanTest::prepareIndex() {
    ASSERT_TRUE(_tableBuilder.build(_testPath));
    _indexApp = _tableBuilder._indexApp;
}

ScanInitParam KhronosDataPointScanTest::createBaseScanInitParam() {
    ScanInitParam param;
    param.catalogName = _tableName;
    param.tableType = KHRONOS_DATA_POINT_TABLE_TYPE;
    param.bizResource = _sqlBizResource.get();
    param.queryResource = _sqlQueryResource.get();
    param.indexInfos = _indexInfos;
    param.memoryPoolResource = &_memoryPoolResource;
    return param;
}

ScanInitParam KhronosDataPointScanTest::createAllSuccessScanInitParam() {
    ScanInitParam param = createBaseScanInitParam();
    // select value + 1 as data, tags['host'] as taghost, tags['app'], timestamp, watermark + 1 as wm
    // from cpu
    // where tags['host'] = literal_or("10.10.10.1|10.10.10.2")
    // and tags['host'] = "10.10.10.1"
    // and tags['app'] = "ha3"
    // and timestamp > 1546272000 and timestamp <= 1546272004
    param.tableName = "cpu";
    param.limit = 1000;
    param.outputFields = {
        "$data",
        "$taghost",
        "$tags['app']",
        "$timestamp",
        "$wm",
    };
    param.outputExprsJson = R"json({
    "$data": {
        "op": "+",
        "params": [
            "$value",
            1
        ],
        "type": "OTHER"
    },
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

    param.conditionJson = R"json({
    "op": "AND",
    "params": [
        {
            "op": "=",
            "params": [
                {
                    "op": "ITEM",
                    "params": [
                        "$tags",
                        "host"
                    ],
                    "type": "OTHER"
                },
                {
                    "op":"literal_or",
                    "params":[
                        "10.10.10.1|10.10.10.2"
                    ],
                    "type":"UDF"
                }
            ],
            "type": "OTHER"
        },
        {
            "op": "=",
            "params": [
                {
                    "op": "ITEM",
                    "params": [
                        "$tags",
                        "host"
                    ],
                    "type": "OTHER"
                },
                "10.10.10.1"
            ],
            "type": "OTHER"
        },
        {
            "op": "=",
            "params": [
                {
                    "op": "ITEM",
                    "params": [
                        "$tags",
                        "app"
                    ],
                    "type": "OTHER"
                },
                "ha3"
            ],
            "type": "OTHER"
        },
        {
            "op": ">=",
            "params": [
                "$timestamp",
                1546272000
            ],
            "type": "OTHER"
        },
        {
            "op": "<=",
            "params": [
                "$timestamp",
                1546272004
            ],
            "type": "OTHER"
        }
    ],
    "type": "OTHER"
})json";

    return param;
}

TEST_F(KhronosDataPointScanTest, testAllSuccess_Simple) {
    ScanInitParam param = createAllSuccessScanInitParam();

    KhronosDataPointScan scan;
    ASSERT_TRUE(scan.init(param));
    // condition
    ASSERT_EQ(1546272000, scan._tsRange.begin);
    ASSERT_EQ(1546272005, scan._tsRange.end);
    auto &tagKVInfos = scan._tagKVInfos;
    ASSERT_EQ(3, tagKVInfos.size());
    ASSERT_EQ(string("host"), tagKVInfos[0].first.toString());
    ASSERT_EQ(string("10.10.10.1|10.10.10.2"), tagKVInfos[0].second.mTagvPattern.toString());
    ASSERT_EQ(SearchInterface::LITERAL_OR, tagKVInfos[0].second.mTagvMatchType);
    ASSERT_EQ(string("host"), tagKVInfos[1].first.toString());
    ASSERT_EQ(string("10.10.10.1"), tagKVInfos[1].second.mTagvPattern.toString());
    ASSERT_EQ(SearchInterface::LITERAL, tagKVInfos[1].second.mTagvMatchType);
    ASSERT_EQ(string("app"), tagKVInfos[2].first.toString());
    ASSERT_EQ(string("ha3"), tagKVInfos[2].second.mTagvPattern.toString());
    ASSERT_EQ(SearchInterface::LITERAL, tagKVInfos[2].second.mTagvMatchType);
    // used fields
    ASSERT_EQ(2, scan._usedFieldsTagkSet.size());
    ASSERT_NE(scan._usedFieldsTagkSet.end(), scan._usedFieldsTagkSet.find("host"));
    ASSERT_NE(scan._usedFieldsTagkSet.end(), scan._usedFieldsTagkSet.find("app"));
    ASSERT_EQ("$timestamp", scan._tsColName);
    ASSERT_EQ("$watermark", scan._watermarkColName);
    ASSERT_EQ("$value", scan._valueColName);
    // metric
    ASSERT_EQ("cpu", scan._metric.toString());
    // lookup result
    ASSERT_NE(nullptr, scan._rowIter);

    auto &exprsMap = scan._calcTable->_exprsMap;
    ASSERT_EQ(3, exprsMap.size());
    ASSERT_NE(exprsMap.end(), exprsMap.find("data"));
    ASSERT_NE(exprsMap.end(), exprsMap.find("taghost"));
    ASSERT_NE(exprsMap.end(), exprsMap.find("wm"));
    auto &exprAliasMap = scan._calcTable->_exprsAliasMap;
    ASSERT_EQ(2, exprAliasMap.size());
    ASSERT_EQ("tags['host']", exprAliasMap["tags__host__"]);
    ASSERT_EQ("tags['app']", exprAliasMap["tags__app__"]);

    TablePtr outputTable;
    bool eof = true;
    ASSERT_TRUE(scan.doBatchScan(outputTable, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_EQ(5, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn<double>(outputTable, "data", {6, 6.1, 6.2}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "taghost", {"10.10.10.1", "10.10.10.1", "10.10.10.1"}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "tags['app']", {"ha3", "ha3", "ha3"}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn<KHR_TS_TYPE>(outputTable, "timestamp", {1546272001, 1546272002, 1546272004}));
    // watermark
    // 1546272048s =
    // 1546272000 (epoch) + 58 (max timestamp) - 10(level1_watermark_latency_in_second)
    // wm: 1546272048001 = 1546272048000(ms) + 1
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn<KHR_WM_TYPE>(outputTable, "wm", {1546272048001, 1546272048001, 1546272048001}));
}

TEST_F(KhronosDataPointScanTest, testAllSuccess_SmallBufferLimit) {
    ScanInitParam param = createAllSuccessScanInitParam();
    KhronosDataPointScan scan;
    ASSERT_TRUE(scan.init(param));
    scan._lineSegmentBuffer->mBufferLimit = 1;

    TablePtr outputTable;
    bool eof = true;
    ASSERT_TRUE(scan.doBatchScan(outputTable, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_EQ(5, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn<double>(outputTable, "data", {6, 6.1, 6.2}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "taghost", {"10.10.10.1", "10.10.10.1", "10.10.10.1"}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "tags['app']", {"ha3", "ha3", "ha3"}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn<KHR_TS_TYPE>(outputTable, "timestamp", {1546272001, 1546272002, 1546272004}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn<KHR_WM_TYPE>(outputTable, "wm", {1546272048001, 1546272048001, 1546272048001}));
}

TEST_F(KhronosDataPointScanTest, testAllSuccess_HasLimit) {
    ScanInitParam param = createAllSuccessScanInitParam();
    param.limit = 2;
    param.batchSize = 1;
    KhronosDataPointScan scan;
    ASSERT_TRUE(scan.init(param));
    // batch 1
    {
        TablePtr outputTable;
        bool eof = true;
        ASSERT_TRUE(scan.doBatchScan(outputTable, eof));
        ASSERT_FALSE(eof);
        ASSERT_EQ(1, outputTable->getRowCount());
        ASSERT_EQ(5, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<double>(outputTable, "data", {6}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "taghost", {"10.10.10.1"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "tags['app']", {"ha3"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<KHR_TS_TYPE>(outputTable, "timestamp", {1546272001}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<KHR_WM_TYPE>(outputTable, "wm", {1546272048001}));
    }

    // batch 2
    {
        TablePtr outputTable;
        bool eof = true;
        ASSERT_TRUE(scan.doBatchScan(outputTable, eof));
        ASSERT_TRUE(eof);
        ASSERT_EQ(1, outputTable->getRowCount());
        ASSERT_EQ(5, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<double>(outputTable, "data", {6.1}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "taghost", {"10.10.10.1"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "tags['app']", {"ha3"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<KHR_TS_TYPE>(outputTable, "timestamp", {1546272002}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<KHR_WM_TYPE>(outputTable, "wm", {1546272048001}));
    }
}

TEST_F(KhronosDataPointScanTest, testAllSuccess_WithBatch) {
    ScanInitParam param = createAllSuccessScanInitParam();
    KhronosDataPointScan scan;
    param.batchSize = 2;
    ASSERT_TRUE(scan.init(param));
    // batch 1
    {
        TablePtr outputTable;
        bool eof = true;
        ASSERT_TRUE(scan.doBatchScan(outputTable, eof));
        ASSERT_FALSE(eof);
        ASSERT_EQ(2, outputTable->getRowCount());
        ASSERT_EQ(5, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<double>(outputTable, "data", {6, 6.1}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "taghost", {"10.10.10.1", "10.10.10.1"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "tags['app']", {"ha3", "ha3"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<KHR_TS_TYPE>(outputTable, "timestamp", {1546272001, 1546272002}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<KHR_WM_TYPE>(outputTable, "wm", {1546272048001, 1546272048001}));
    }
    // batch 2
    {
        TablePtr outputTable;
        bool eof = true;
        ASSERT_TRUE(scan.doBatchScan(outputTable, eof));
        ASSERT_TRUE(eof);
        ASSERT_EQ(1, outputTable->getRowCount());
        ASSERT_EQ(5, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<double>(outputTable, "data", {6.2}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "taghost", {"10.10.10.1"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "tags['app']", {"ha3"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<KHR_TS_TYPE>(outputTable, "timestamp", {1546272004}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<KHR_WM_TYPE>(outputTable, "wm", {1546272048001}));
    }
}

TEST_F(KhronosDataPointScanTest, testAllSuccess_EmptyBatch) {
    ScanInitParam param = createAllSuccessScanInitParam();
    KhronosDataPointScan scan;
    param.batchSize = 3;
    ASSERT_TRUE(scan.init(param));
    // batch 1
    {
        TablePtr outputTable;
        bool eof = true;
        ASSERT_TRUE(scan.doBatchScan(outputTable, eof));
        ASSERT_FALSE(eof);
        ASSERT_EQ(3, outputTable->getRowCount());
        ASSERT_EQ(5, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<double>(outputTable, "data", {6, 6.1, 6.2}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "taghost", {"10.10.10.1", "10.10.10.1", "10.10.10.1"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "tags['app']", {"ha3", "ha3", "ha3"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<KHR_TS_TYPE>(outputTable, "timestamp", {1546272001, 1546272002, 1546272004}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<KHR_WM_TYPE>(outputTable, "wm", {1546272048001, 1546272048001, 1546272048001}));
    }
    // batch 2
    {
        TablePtr outputTable;
        bool eof = true;
        ASSERT_TRUE(scan.doBatchScan(outputTable, eof));
        ASSERT_TRUE(eof);
        ASSERT_EQ(0, outputTable->getRowCount());
        ASSERT_EQ(5, outputTable->getColumnCount());
    }
}

TEST_F(KhronosDataPointScanTest, testAllError_TimeoutTerminator) {
    auto param = createAllSuccessScanInitParam();
    KhronosDataPointScan scan;
    ASSERT_TRUE(scan.init(param));
    scan._timeoutTerminator.reset(new TimeoutTerminator(1, 0));
    scan._timeoutTerminator->init(1 << 12);

    TablePtr outputTable;
    bool eof = true;

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.doBatchScan(outputTable, eof));

    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "run timeout", traces);
}

TEST_F(KhronosDataPointScanTest, init_Success) {
    ScanInitParam param = createBaseScanInitParam();
    param.conditionJson = R"json({
    "op": "AND",
 "params": [
        {
            "op": ">=",
            "params": [
                "$timestamp",
                1546272004
            ],
            "type": "OTHER"
        }
    ],
    "type": "OTHER"
})json";
    param.outputExprsJson = R"json({})json";
    param.outputFields = {
        "$value"
    };

    navi::NaviLoggerProvider provider("TRACE3");
    KhronosDataPointScan scan;
    ASSERT_TRUE(scan.init(param));
    ASSERT_NE(nullptr, scan._rowIter);
    ASSERT_EQ("$value", scan._valueColName);
    ASSERT_EQ(1546272004, scan._tsRange.begin);
}

TEST_F(KhronosDataPointScanTest, extractUsedField_Error_DuplicateTimestampFieldName) {
    OutputFieldsVisitor visitor;
    visitor._usedFieldsColumnSet = {
        "$timestamp1",
        "$timestamp2",
        "$value",
    };

    KhronosDataPointScan scan;

    string indexInfosStr = R"json({
    "$value": {
        "name": "value",
        "type": "khronos_value"
    },
    "$timestamp1": {
        "name": "timestamp1",
        "type": "khronos_timestamp"
    },
    "$timestamp2": {
        "name": "timestamp2",
        "type": "khronos_timestamp"
    }
})json";
    FromJsonString(scan._indexInfos, indexInfosStr);

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.extractUsedField(visitor));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "duplicate timestamp field name: [$timestamp1] vs [$timestamp2]", traces);
}

TEST_F(KhronosDataPointScanTest, extractUsedField_Error_DuplicateWatermarkField) {
    OutputFieldsVisitor visitor;
    visitor._usedFieldsColumnSet = {
        "$watermark1",
        "$watermark2",
    };

    KhronosDataPointScan scan;

    string indexInfosStr = R"json({
    "$watermark1": {
        "name": "watermark1",
        "type": "khronos_watermark"
    },
    "$watermark2": {
        "name": "watermark2",
        "type": "khronos_watermark"
    }
})json";
    FromJsonString(scan._indexInfos, indexInfosStr);

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.extractUsedField(visitor));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(
            1, "only one watermark field is supported: [$watermark1] vs [$watermark2]", traces);
}

TEST_F(KhronosDataPointScanTest, extractUsedField_Error_DuplicateValueField) {
    OutputFieldsVisitor visitor;
    visitor._usedFieldsColumnSet = {
        "$timestamp",
        "$value1",
        "$value2",
    };

    KhronosDataPointScan scan;

    string indexInfosStr = R"json({
    "$value1": {
        "name": "value1",
        "type": "khronos_value"
    },
    "$value2": {
        "name": "value2",
        "type": "khronos_value"
    },
    "$timestamp": {
        "name": "timestamp",
        "type": "khronos_timestamp"
    }
})json";
    FromJsonString(scan._indexInfos, indexInfosStr);

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.extractUsedField(visitor));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "only one value field is supported: [$value1] vs [$value2]", traces);
}

TEST_F(KhronosDataPointScanTest, extractUsedField_Error_NotSupportedField) {
    OutputFieldsVisitor visitor;
    visitor._usedFieldsColumnSet = {
        "$invalid",
    };

    KhronosDataPointScan scan;
    scan._indexInfos = _indexInfos;

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.extractUsedField(visitor));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "field [$invalid] is not supported in khronos data point scan", traces);
}

TEST_F(KhronosDataPointScanTest, extractUsedField_Error_EmptyValueField) {
    OutputFieldsVisitor visitor;
    visitor._usedFieldsColumnSet = {
        "$timestamp",
    };

    KhronosDataPointScan scan;
    scan._indexInfos = _indexInfos;

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.extractUsedField(visitor));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "value column field must be used in query", traces);
}

TEST_F(KhronosDataPointScanTest, extractUsedField_Error_TagsNameMismatch) {
    OutputFieldsVisitor visitor;
    visitor._usedFieldsItemSet = {
        {"$prefix", "host"},
    };
    visitor._usedFieldsColumnSet = {
        "$value",
    };

    KhronosDataPointScan scan;
    scan._indexInfos = _indexInfos;

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.extractUsedField(visitor));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "tags name mismatch: [$prefix]", traces);
}

TEST_F(KhronosDataPointScanTest, extractUsedField_Success) {
    OutputFieldsVisitor visitor;
    visitor._usedFieldsItemSet = {
        {"$tags", "host"},
    };
    visitor._usedFieldsColumnSet = {
        "$timestamp",
        "$watermark",
        "$value",
    };

    KhronosDataPointScan scan;
    scan._indexInfos = _indexInfos;

    navi::NaviLoggerProvider provider("TRACE3");
    ASSERT_TRUE(scan.extractUsedField(visitor));
    ASSERT_EQ("$value", scan._valueColName);
    ASSERT_EQ("$timestamp", scan._tsColName);
    auto &usedFieldsTagkSet = scan._usedFieldsTagkSet;
    ASSERT_EQ(1, usedFieldsTagkSet.size());
    ASSERT_NE(usedFieldsTagkSet.end(), usedFieldsTagkSet.find("host"));

    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "data point scan used fields: ts=[$timestamp], watermark=[$watermark],"
                      " value=[$value], tagk=[host]", traces);
}

TEST_F(KhronosDataPointScanTest, parseUsedFields_Success) {
    KhronosDataPointScan scan;
    scan._pool = &_pool;
    scan._kernelPool = _memPoolResource.getPool();
    scan._indexInfos = _indexInfos;
    scan._outputFields = {
        "$value",
        "$taghost",
    };
    scan._outputExprsJson = R"json({
    "$taghost": {
        "op": "ITEM",
        "params": [
            "$tags",
            "host"
        ],
        "type": "OTHER"
    }
})json";
    ASSERT_TRUE(scan.parseUsedFields());
    ASSERT_EQ("$value", scan._valueColName);
    ASSERT_EQ("", scan._tsColName);
    auto &usedFieldsTagkSet = scan._usedFieldsTagkSet;
    ASSERT_EQ(1, usedFieldsTagkSet.size());
    ASSERT_NE(usedFieldsTagkSet.end(), usedFieldsTagkSet.find("host"));
}

TEST_F(KhronosDataPointScanTest, parseQueryParamsFromCondition_Error_VisitAndParseConditionFailed) {
    KhronosDataPointScan scan;
    scan._pool = &_pool;
    scan._kernelPool = _memPoolResource.getPool();
    scan._indexInfos = _indexInfos;
    scan._conditionJson = R"json({})json";

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.parseQueryParamsFromCondition());
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "visit and parse condition failed", traces);
}

TEST_F(KhronosDataPointScanTest, parseQueryParamsFromCondition_Error_InvalidTsRange) {
    KhronosDataPointScan scan;
    scan._pool = &_pool;
    scan._kernelPool = _memPoolResource.getPool();
    scan._indexInfos = _indexInfos;
    scan._conditionJson = R"json({
    "op": "AND",
    "params": [
        {
            "op": ">",
            "params": [
                "$timestamp",
                1546272004
            ],
            "type": "OTHER"
        },
        {
            "op": "<",
            "params": [
                "$timestamp",
                1546272004
            ],
            "type": "OTHER"
        }
    ],
    "type": "OTHER"
})json";

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.parseQueryParamsFromCondition());
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "time stamp range is invalid", traces);
}

TEST_F(KhronosDataPointScanTest, parseQueryParamsFromCondition_Success) {
    KhronosDataPointScan scan;
    scan._pool = &_pool;
    scan._kernelPool = _memPoolResource.getPool();
    scan._indexInfos = _indexInfos;
    scan._conditionJson = R"json({
    "op": "AND",
    "params": [
        {
            "op": ">=",
            "params": [
                "$timestamp",
                1546272004
            ],
            "type": "OTHER"
        },
        {
            "op":"LIKE",
            "params":[
                {
                    "op":"ITEM",
                    "params":[
                        "$tags",
                        "host"
                    ],
                    "type":"OTHER"
                },
                "10.10*"
            ],
            "type":"OTHER"
        }
    ],
    "type": "OTHER"
})json";

    navi::NaviLoggerProvider provider("TRACE3");
    ASSERT_TRUE(scan.parseQueryParamsFromCondition());
    ASSERT_EQ(1546272004, scan._tsRange.begin);
    auto &tagKVInfos = scan._tagKVInfos;
    ASSERT_EQ(1, tagKVInfos.size());
    ASSERT_EQ(1, tagKVInfos.size());
    ASSERT_EQ(string("host"), tagKVInfos[0].first.c_str());
    ASSERT_EQ(string("10.10*"), tagKVInfos[0].second.mTagvPattern.c_str());
    ASSERT_EQ(SearchInterface::LITERAL, tagKVInfos[0].second.mTagvMatchType);

    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "data point scan condition:", traces);
}

END_HA3_NAMESPACE();
