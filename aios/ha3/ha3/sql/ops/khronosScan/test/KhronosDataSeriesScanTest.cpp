#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/khronosScan/test/KhronosScanTestUtil.h>
#include <ha3/sql/ops/khronosScan/KhronosCommon.h>
#include <ha3/sql/ops/khronosScan/KhronosScanKernel.h>
#include <ha3/sql/ops/khronosScan/KhronosDataSeriesScan.h>
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
using namespace khronos;
using namespace khronos::search;
using namespace khronos::indexlib_plugin;

BEGIN_HA3_NAMESPACE(sql);

class KhronosDataSeriesScanTest : public OpTestBase {
public:
    KhronosDataSeriesScanTest()
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

HA3_LOG_SETUP(searcher, KhronosDataSeriesScanTest);

void KhronosDataSeriesScanTest::setUp() {
    _needBuildIndex = true;
    _needExprResource = false;

    string indexInfosStr = R"json({
    "$value": {
        "name": "value",
        "type": "khronos_series"
    },
    "$watermark": {
        "name": "watermark",
        "type": "khronos_watermark"
    }
})json";
    FromJsonString(_indexInfos, indexInfosStr);
}

void KhronosDataSeriesScanTest::tearDown() {
    clearResource();
    _indexApp.reset();
}

void KhronosDataSeriesScanTest::prepareIndex() {
    ASSERT_TRUE(_tableBuilder.build(_testPath));
    _indexApp = _tableBuilder._indexApp;
}

ScanInitParam KhronosDataSeriesScanTest::createBaseScanInitParam() {
    ScanInitParam param;
    param.catalogName = _tableName;
    param.tableType = KHRONOS_DATA_SERIES_TABLE_TYPE;
    param.bizResource = _sqlBizResource.get();
    param.queryResource = _sqlQueryResource.get();
    param.indexInfos = _indexInfos;
    param.memoryPoolResource = &_memoryPoolResource;
    return param;
}

ScanInitParam KhronosDataSeriesScanTest::createAllSuccessScanInitParam() {
    ScanInitParam param = createBaseScanInitParam();
    // select value as data, tags['host'] as taghost, tags['app'], watermark + 1 as wm from cpu
    // where tags['host'] = literal_or("10.10.10.1|10.10.10.2")
    // and tags['app'] = "bs"
    // and timestamp_larger_equal_than(1546272000)
    // and timestamp_less_than(1546272025)
    param.tableName = "cpu";
    param.limit = 1000;
    param.outputFields = {
        "$data",
        "$taghost",
        "$tags['app']",
        "$wm",
    };
    param.outputExprsJson = R"json({
    "$data": "$value",
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
                        "app"
                    ],
                    "type": "OTHER"
                },
                "bs"
            ],
            "type": "OTHER"
        },
        {
            "op":
            "timestamp_larger_equal_than",
            "params":
            [
                1546272000
            ],
            "type":
            "UDF"
        },
        {
            "op":
            "timestamp_less_than",
            "params":
            [
                1546272025
            ],
            "type":
            "UDF"
        }
    ],
    "type": "OTHER"
})json";

    return param;
}

TEST_F(KhronosDataSeriesScanTest, testAllSuccess_Simple) {
    auto param = createAllSuccessScanInitParam();
    KhronosDataSeriesScan scan;
    ASSERT_TRUE(scan.init(param));
    // condition
    ASSERT_EQ(1546272000, scan._tsRange.begin);
    ASSERT_EQ(1546272025, scan._tsRange.end);
    auto &tagKVInfos = scan._tagKVInfos;
    ASSERT_EQ(2, tagKVInfos.size());
    ASSERT_EQ(string("host"), tagKVInfos[0].first.toString());
    ASSERT_EQ(string("10.10.10.1|10.10.10.2"), tagKVInfos[0].second.mTagvPattern.toString());
    ASSERT_EQ(SearchInterface::LITERAL_OR, tagKVInfos[0].second.mTagvMatchType);
    ASSERT_EQ(string("app"), tagKVInfos[1].first.toString());
    ASSERT_EQ(string("bs"), tagKVInfos[1].second.mTagvPattern.toString());
    ASSERT_EQ(SearchInterface::LITERAL, tagKVInfos[1].second.mTagvMatchType);
    // used fields
    ASSERT_EQ(2, scan._usedFieldsTagkSet.size());
    ASSERT_NE(scan._usedFieldsTagkSet.end(), scan._usedFieldsTagkSet.find("host"));
    ASSERT_NE(scan._usedFieldsTagkSet.end(), scan._usedFieldsTagkSet.find("app"));
    ASSERT_EQ("$value", scan._valueColName);
    ASSERT_EQ("$watermark", scan._watermarkColName);
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
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "taghost", {"10.10.10.1", "10.10.10.2"}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "tags['app']", {"bs", "bs"}));
    // watermark
    // 1546272048s =
    // 1546272000 (epoch) + 58 (max timestamp) - 10(level1_watermark_latency_in_second)
    // wm: 1546272048001 = 1546272048000(ms) + 1
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn<KHR_WM_TYPE>(outputTable, "wm", {1546272048001, 1546272048001}));
    ASSERT_NO_FATAL_FAILURE(
            KhronosScanTestUtil::checkDataPointList(outputTable, "data", {
                        {{1546272009,6.0}, {1546272010, 6.0}, {1546272013, 6.0}},
                        {{1546272020,6.0}, {1546272022, 6.0}}
                    }));
}

TEST_F(KhronosDataSeriesScanTest, testAllSuccess_SmallBufferLimit) {
    auto param = createAllSuccessScanInitParam();
    KhronosDataSeriesScan scan;
    ASSERT_TRUE(scan.init(param));
    scan._lineSegmentBuffer->mBufferLimit = 1;

    TablePtr outputTable;
    bool eof = true;
    ASSERT_TRUE(scan.doBatchScan(outputTable, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "taghost", {"10.10.10.1", "10.10.10.2"}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "tags['app']", {"bs", "bs"}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn<KHR_WM_TYPE>(outputTable, "wm", {1546272048001, 1546272048001}));
    ASSERT_NO_FATAL_FAILURE(
            KhronosScanTestUtil::checkDataPointList(outputTable, "data", {
                        {{1546272009,6.0}, {1546272010, 6.0}, {1546272013, 6.0}},
                        {{1546272020,6.0}, {1546272022, 6.0}}
                    }));
}

TEST_F(KhronosDataSeriesScanTest, testAllSuccess_HasLimit) {
    auto param = createAllSuccessScanInitParam();
    param.limit = 1;
    KhronosDataSeriesScan scan;
    ASSERT_TRUE(scan.init(param));

    TablePtr outputTable;
    bool eof = true;
    ASSERT_TRUE(scan.doBatchScan(outputTable, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "taghost", {"10.10.10.1"}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "tags['app']", {"bs"}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn<KHR_WM_TYPE>(outputTable, "wm", {1546272048001}));
    ASSERT_NO_FATAL_FAILURE(
            KhronosScanTestUtil::checkDataPointList(outputTable, "data", {
                        {{1546272009,6.0}, {1546272010, 6.0}, {1546272013, 6.0}},
                    }));
}

TEST_F(KhronosDataSeriesScanTest, testAllSuccess_MultiBatch) {
    auto param = createAllSuccessScanInitParam();
    KhronosDataSeriesScan scan;
    param.batchSize = 1;
    ASSERT_TRUE(scan.init(param));
    // batch 1
    {
        TablePtr outputTable;
        bool eof = true;
        ASSERT_TRUE(scan.doBatchScan(outputTable, eof));
        ASSERT_FALSE(eof);
        ASSERT_EQ(1, outputTable->getRowCount());
        ASSERT_EQ(4, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(
                KhronosScanTestUtil::checkDataPointList(outputTable, "data", {
                            {{1546272009,6.0}, {1546272010, 6.0}, {1546272013, 6.0}}
                        }));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "taghost", {"10.10.10.1"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "tags['app']", {"bs"}));
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
        ASSERT_EQ(4, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(
                KhronosScanTestUtil::checkDataPointList(outputTable, "data", {
                            {{1546272020,6.0}, {1546272022, 6.0}}
                        }));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "taghost", {"10.10.10.2"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "tags['app']", {"bs"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<KHR_WM_TYPE>(outputTable, "wm", {1546272048001}));
    }

}

TEST_F(KhronosDataSeriesScanTest, testAllSuccess_EveryBatchSize) {
    for (size_t size = 0; size < 512; ++size) {
        auto param = createAllSuccessScanInitParam();
        KhronosDataSeriesScan scan;
        param.batchSize = size;
        ASSERT_TRUE(scan.init(param));

        TablePtr resultTable;
        while (true) {
            TablePtr outputTable;
            bool eof = true;
            ASSERT_TRUE(scan.doBatchScan(outputTable, eof));
            ASSERT_EQ(4, outputTable->getColumnCount());
            if (!resultTable) {
                resultTable = outputTable;
            } else {
                ASSERT_TRUE(resultTable->merge(outputTable));
            }
            if (eof) {
                break;
            }
        }
        ASSERT_EQ(2, resultTable->getRowCount());
        ASSERT_EQ(4, resultTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(resultTable, "taghost", {"10.10.10.1", "10.10.10.2"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(resultTable, "tags['app']", {"bs", "bs"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<KHR_WM_TYPE>(resultTable, "wm", {1546272048001, 1546272048001}));
        ASSERT_NO_FATAL_FAILURE(
                KhronosScanTestUtil::checkDataPointList(resultTable, "data", {
                            {{1546272009,6.0}, {1546272010, 6.0}, {1546272013, 6.0}},
                            {{1546272020,6.0}, {1546272022, 6.0}}
                        }));
    }
}

TEST_F(KhronosDataSeriesScanTest, testAllError_ExceedDataPoolLimit) {
    auto param = createAllSuccessScanInitParam();
    KhronosDataSeriesScan scan;
    ASSERT_TRUE(scan.init(param));
    scan._dataPoolBytesLimit = 1;

    TablePtr outputTable;
    bool eof = true;

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.doBatchScan(outputTable, eof));

    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "pool used too many bytes, limit=[1], actual=", traces);
}

TEST_F(KhronosDataSeriesScanTest, testAllError_ExceedHintLimit) {
    auto param = createAllSuccessScanInitParam();
    KhronosScanHints scanHints;
    scanHints.oneLineScanPointLimit = 1;
    KhronosDataSeriesScan scan(scanHints);
    ASSERT_TRUE(scan.init(param));

    TablePtr outputTable;
    bool eof = true;

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.doBatchScan(outputTable, eof));

    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "current line point scan count exceeded, limit=[1], actual=", traces);
}

TEST_F(KhronosDataSeriesScanTest, testAllError_TimeoutTerminator) {
    auto param = createAllSuccessScanInitParam();
    KhronosDataSeriesScan scan;
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

TEST_F(KhronosDataSeriesScanTest, extractUsedField_Error_DuplicateValueField) {
    OutputFieldsVisitor visitor;
    visitor._usedFieldsColumnSet = {
        "$value1",
        "$value2",
    };

    KhronosDataSeriesScan scan;

    string indexInfosStr = R"json({
    "$value1": {
        "name": "value1",
        "type": "khronos_series"
    },
    "$value2": {
        "name": "value2",
        "type": "khronos_series"
    }
})json";
    FromJsonString(scan._indexInfos, indexInfosStr);

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.extractUsedField(visitor));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "only one value field is supported: [$value1] vs [$value2]", traces);
}

TEST_F(KhronosDataSeriesScanTest, extractUsedField_Error_DuplicateWatermarkField) {
    OutputFieldsVisitor visitor;
    visitor._usedFieldsColumnSet = {
        "$watermark1",
        "$watermark2",
    };

    KhronosDataSeriesScan scan;

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

TEST_F(KhronosDataSeriesScanTest, extractUsedField_Error_NotSupportedField) {
    OutputFieldsVisitor visitor;
    visitor._usedFieldsColumnSet = {
        "$invalid",
    };

    KhronosDataSeriesScan scan;
    scan._indexInfos = _indexInfos;

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.extractUsedField(visitor));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "field [$invalid] is not supported in khronos data series scan", traces);
}

TEST_F(KhronosDataSeriesScanTest, extractUsedField_Error_EmptyValueField) {
    OutputFieldsVisitor visitor;
    visitor._usedFieldsColumnSet = {
    };

    KhronosDataSeriesScan scan;
    scan._indexInfos = _indexInfos;

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.extractUsedField(visitor));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "value column field must be used in query", traces);
}

TEST_F(KhronosDataSeriesScanTest, extractUsedField_Error_TagsNameMismatch) {
    OutputFieldsVisitor visitor;
    visitor._usedFieldsItemSet = {
        {"$prefix", "host"},
    };
    visitor._usedFieldsColumnSet = {
        "$value",
    };

    KhronosDataSeriesScan scan;
    scan._indexInfos = _indexInfos;

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.extractUsedField(visitor));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "tags name mismatch: [$prefix]", traces);
}

TEST_F(KhronosDataSeriesScanTest, extractUsedField_Success) {
    OutputFieldsVisitor visitor;
    visitor._usedFieldsItemSet = {
        {"$tags", "host"},
    };
    visitor._usedFieldsColumnSet = {
        "$value",
        "$watermark",
    };

    KhronosDataSeriesScan scan;
    scan._indexInfos = _indexInfos;

    navi::NaviLoggerProvider provider("TRACE3");
    ASSERT_TRUE(scan.extractUsedField(visitor));
    ASSERT_EQ("$value", scan._valueColName);
    auto &usedFieldsTagkSet = scan._usedFieldsTagkSet;
    ASSERT_EQ(1, usedFieldsTagkSet.size());
    ASSERT_NE(usedFieldsTagkSet.end(), usedFieldsTagkSet.find("host"));

    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "data series scan used fields: value=[$value], watermark=[$watermark], tagk=[host]", traces);
}

TEST_F(KhronosDataSeriesScanTest, parseUsedFields_Success) {
    KhronosDataSeriesScan scan;
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
    auto &usedFieldsTagkSet = scan._usedFieldsTagkSet;
    ASSERT_EQ(1, usedFieldsTagkSet.size());
    ASSERT_NE(usedFieldsTagkSet.end(), usedFieldsTagkSet.find("host"));
}

TEST_F(KhronosDataSeriesScanTest, parseQueryParamsFromCondition_Error_VisitAndParseConditionFailed) {
    KhronosDataSeriesScan scan;
    scan._pool = &_pool;
    scan._kernelPool = _memPoolResource.getPool();
    scan._indexInfos = _indexInfos;
    scan._conditionJson = R"json({})json";

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.parseQueryParamsFromCondition());
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "visit and parse condition failed", traces);
}

TEST_F(KhronosDataSeriesScanTest, parseQueryParamsFromCondition_Error_InvalidTsRange) {
    KhronosDataSeriesScan scan;
    scan._pool = &_pool;
    scan._kernelPool = _memPoolResource.getPool();
    scan._indexInfos = _indexInfos;
    scan._conditionJson = R"json({
    "op": "AND",
    "params": [
        {
            "op":
            "timestamp_larger_equal_than",
            "params":
            [
                1546272004
            ],
            "type":
            "UDF"
        },
        {
            "op":
            "timestamp_less_than",
            "params":
            [
                1546272004
            ],
            "type":
            "UDF"
        }
    ],
    "type": "OTHER"
})json";

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.parseQueryParamsFromCondition());
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "time stamp range is invalid", traces);
}

TEST_F(KhronosDataSeriesScanTest, parseQueryParamsFromCondition_Success) {
    KhronosDataSeriesScan scan;
    scan._pool = &_pool;
    scan._kernelPool = _memPoolResource.getPool();
    scan._indexInfos = _indexInfos;
    scan._conditionJson = R"json({
    "op": "AND",
    "params": [
        {
            "op":
            "timestamp_larger_equal_than",
            "params":
            [
                1546272004
            ],
            "type":
            "UDF"
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
    CHECK_TRACE_COUNT(1, "data series scan condition:", traces);
}

END_HA3_NAMESPACE();
