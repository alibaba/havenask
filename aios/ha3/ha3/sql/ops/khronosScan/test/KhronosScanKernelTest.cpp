#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/khronosScan/test/KhronosScanTestUtil.h>
#include <ha3/sql/ops/khronosScan/KhronosCommon.h>
#include <ha3/sql/ops/khronosScan/KhronosScanKernel.h>
#include <ha3/sql/ops/khronosScan/KhronosMetaScan.h>
#include <ha3/sql/ops/khronosScan/KhronosDataPointScan.h>
#include <ha3/sql/ops/khronosScan/KhronosDataSeriesScan.h>
#include <ha3/sql/ops/khronosScan/test/KhronosTestTableBuilder.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/turing/ops/SqlSessionResource.h>
#include <ha3/search/IndexPartitionReaderUtil.h>
#include <autil/DefaultHashFunction.h>
#include <autil/StringUtil.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <navi/resource/MemoryPoolResource.h>
#include <matchdoc/MatchDocAllocator.h>

using namespace std;
using namespace matchdoc;
using namespace testing;
using namespace suez::turing;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace navi;
using namespace khronos::search;
using namespace khronos::indexlib_plugin;

BEGIN_HA3_NAMESPACE(sql);

class KhronosScanKernelTest : public OpTestBase {
public:
    KhronosScanKernelTest()
    {
        _tableName = "simple_table";
    }
    void setUp();
    void tearDown();
public:
    void prepareIndex() override;
private:
    KernelTesterPtr buildTester(KernelTesterBuilder &builder);
    void prepareAttributeMap();
    void prepareDataPointScanAttributeMap();
    void prepareDataSeriesScanAttributeMap();
    template <typename T, typename Func>
    void checkKhronosDataScanInfo(KernelTester &tester, Func f);
    void computeKernel(
        KernelTester &tester,
        TablePtr &outputTable,
        bool &eof);
private:
    MemoryPoolResource _memoryPoolResource;
    IndexInfoMapType _indexInfos;
    KhronosTestTableBuilder _tableBuilder;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(searcher, KhronosScanKernelTest);

void KhronosScanKernelTest::setUp() {
    _needBuildIndex = true;
    _needExprResource = false;
}

void KhronosScanKernelTest::tearDown() {
    clearResource();
    _indexApp.reset();
}

void KhronosScanKernelTest::prepareIndex() {
    ASSERT_TRUE(_tableBuilder.build(_testPath));
    _indexApp = _tableBuilder._indexApp;
}

template <typename T, typename Func>
void KhronosScanKernelTest::checkKhronosDataScanInfo(KernelTester &tester, Func f) {
    auto *kernel = dynamic_cast<KhronosScanKernel *>(tester.getKernel());
    ASSERT_NE(nullptr, kernel);
    auto *scan = dynamic_cast<T *>(kernel->_scanBase.get());
    ASSERT_NE(nullptr, scan);
    ASSERT_NO_FATAL_FAILURE(f(scan->_khronosDataScanInfo));
}

KernelTesterPtr KhronosScanKernelTest::buildTester(KernelTesterBuilder &builder)
{
    for (auto &resource : _resourceMap) {
        builder.resource(resource.first, resource.second);
    }
    builder.resource(RESOURCE_MEMORY_POOL_URI, &_memPoolResource);
    builder.kernel("KhronosTableScanKernel");
    builder.output("output0");
    builder.attrs(autil::legacy::ToJsonString(_attributeMap));
    return KernelTesterPtr(builder.build());
}

void KhronosScanKernelTest::prepareAttributeMap() {
    _attributeMap.clear();
    _attributeMap["catalog_name"] = _tableName;
    _attributeMap["limit"] = Any(10000);
    _attributeMap["index_infos"] = ParseJson(string(R"json({
    "$tagv" : {
        "name" : "tagv",
        "type" : "khronos_tag_value"
    },
    "$metric" : {
        "name" : "metric",
        "type" : "khronos_metric"
    },
    "$value": {
        "name": "value",
        "type": "khronos_value"
    },
    "$ts" : {
        "name" : "ts",
        "type" : "khronos_timestamp"
    },
    "$tagk" : {
        "name" : "tagk",
        "type" : "khronos_tag_key"
    }
})json"));
    // not used
    _attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    _attributeMap["table_name"] = string("no use table name");
    _attributeMap["db_name"] = string("no use db name");
}

void KhronosScanKernelTest::computeKernel(
        KernelTester &tester,
        TablePtr &outputTable,
        bool &eof)
{
    ASSERT_TRUE(tester.compute()); // kernel compute success
    DataPtr outputData;
    ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    outputTable = getTable(outputData);
    ASSERT_TRUE(outputTable != nullptr);
}

TEST_F(KhronosScanKernelTest, DISABLED_testMetaTagvNormalWithTagk) {
    prepareAttributeMap();
    // select tagv from khronos_meta where tagk = 'host'
    _attributeMap["table_type"] = KHRONOS_META_TABLE_TYPE;
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$tagv"])json"));
    _attributeMap["condition"] = ParseJson(string(R"json({
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
})json"));

    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    TablePtr outputTable;
    bool eof;
    ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_EQ(1, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "tagv", {"10.10.10.1", "10.10.10.2", "10.10.10.3"}));
}

TEST_F(KhronosScanKernelTest, DISABLED_testMetaTagvNormalWithTagkTagv) {
    prepareAttributeMap();
    // select tagv from khronos_meta where tagk = 'host' and tagv = '10.10*'
    _attributeMap["table_type"] = KHRONOS_META_TABLE_TYPE;
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$tagv"])json"));
    _attributeMap["condition"] = ParseJson(string(R"json({
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
})json"));

    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    TablePtr outputTable;
    bool eof;
    ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_EQ(1, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "tagv", {"10.10.10.1", "10.10.10.2", "10.10.10.3"}));
}

TEST_F(KhronosScanKernelTest, testMetaTagvNormalWithMetricTagk) {
    prepareAttributeMap();
    // select tagv from khronos_meta where tagk = 'host' and metric = 'cpu'
    _attributeMap["table_type"] = KHRONOS_META_TABLE_TYPE;
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$tagv"])json"));
    _attributeMap["condition"] = ParseJson(string(R"json({
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
                "$metric",
                "cpu"
            ]
        }
    ]
})json"));

    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    TablePtr outputTable;
    bool eof;
    ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_EQ(1, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "tagv", {"10.10.10.1", "10.10.10.2"}));
}

TEST_F(KhronosScanKernelTest, testMetaTagvNormalWithMetricTagkAndHintLimit) {
    prepareAttributeMap();
    // select /*+ SCAN_ATTR(localLimit="2") */ tagv from khronos_meta where tagk = 'host'
    _attributeMap["table_type"] = KHRONOS_META_TABLE_TYPE;
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$tagv"])json"));
    _attributeMap["condition"] = ParseJson(string(R"json({
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
                "$metric",
                "cpu"
            ]
        }
    ]
})json"));
    _attributeMap["hints"] = ParseJson(R"json({"SCAN_ATTR":{"localLimit":"2"}})json");

    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    TablePtr outputTable;
    bool eof;
    ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_EQ(1, outputTable->getColumnCount());
    ASSERT_NE(nullptr, outputTable->getColumn("tagv"));
}

TEST_F(KhronosScanKernelTest, testMetaTagvNormalWithMetricTagkTagv) {
    prepareAttributeMap();
    // select tagv from khronos_meta where tagk = 'host' and metric = 'cpu' and tagv = '10.10*'
    _attributeMap["table_type"] = KHRONOS_META_TABLE_TYPE;
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$tagv"])json"));
    _attributeMap["condition"] = ParseJson(string(R"json({
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
})json"));

    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    TablePtr outputTable;
    bool eof;
    ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_EQ(1, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "tagv", {"10.10.10.1", "10.10.10.2"}));
}

TEST_F(KhronosScanKernelTest, testMetaTagkNormalWithMetric) {
    prepareAttributeMap();
    // select tagk from khronos_meta where and metric = 'cpu'
    _attributeMap["table_type"] = KHRONOS_META_TABLE_TYPE;
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$tagk"])json"));
    _attributeMap["condition"] = ParseJson(string(R"json({
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
})json"));

    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    TablePtr outputTable;
    bool eof;
    ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_EQ(1, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "tagk", {"app", "host"}));
}

TEST_F(KhronosScanKernelTest, DISABLED_testMetaTagkNormalSimple) {
    prepareAttributeMap();
    // select tagk from khronos_meta
    _attributeMap["table_type"] = KHRONOS_META_TABLE_TYPE;
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$tagk"])json"));
    _attributeMap["condition"] = ParseJson(string(R"json({
    "op" : "AND",
    "params" :
    [
    ]
})json"));

    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    TablePtr outputTable;
    bool eof;
    ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_EQ(1, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "tagk", {"app", "cluster", "host"}));
}

TEST_F(KhronosScanKernelTest, testMetaMetricNormalSimple) {
    prepareAttributeMap();
    // select metric from khronos_meta
    _attributeMap["table_type"] = KHRONOS_META_TABLE_TYPE;
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$metric"])json"));
    _attributeMap["condition"] = ParseJson(string(R"json({
    "op" : "AND",
    "params" :
    [
    ]
})json"));

    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    TablePtr outputTable;
    bool eof;
    ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_EQ(1, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "metric", {"cpu", "mem"}));
}

TEST_F(KhronosScanKernelTest, testMetaMetricNormalWithHintLimit) {
    prepareAttributeMap();
    // select /*+ SCAN_ATTR(localLimit="1") */ metric from khronos_meta
    _attributeMap["table_type"] = KHRONOS_META_TABLE_TYPE;
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$metric"])json"));
    _attributeMap["condition"] = ParseJson(string(R"json({
    "op" : "AND",
    "params" :
    [
    ]
})json"));
    _attributeMap["hints"] = ParseJson(R"json({"SCAN_ATTR":{"localLimit":"1"}})json");

    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    TablePtr outputTable;
    bool eof;
    ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(1, outputTable->getColumnCount());
    ASSERT_NE(nullptr, outputTable->getColumn("metric"));
}

TEST_F(KhronosScanKernelTest, testMetaMetricNormalWithTs) {
    prepareAttributeMap();
    // select metric from khronos_meta where ts >= @stratTs and ts <= @endTs
    auto startTs = khronos::TimeUtil::KhronosTimeToUnixTimeInSecond(30);
    auto endTs = khronos::TimeUtil::KhronosTimeToUnixTimeInSecond(60);
    string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : ">=",
            "params" :
            [
                "$ts",
                @startTs
            ]
        },
        {
            "op" : "<=",
            "params" :
            [
                "$ts",
                @endTs
            ]
        }
    ]
})json";
    StringUtil::replaceAll(conditionJson, "@startTs", to_string(startTs));
    StringUtil::replaceAll(conditionJson, "@endTs", to_string(endTs));

    // select metric from khronos_meta where
    _attributeMap["table_type"] = KHRONOS_META_TABLE_TYPE;
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$metric"])json"));
    _attributeMap["condition"] = ParseJson(conditionJson);
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    TablePtr outputTable;
    bool eof;
    ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(1, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "metric", {"mem"}));
}

TEST_F(KhronosScanKernelTest, testMetaMetricNormalWithMetricPrefix) {
    prepareAttributeMap();
    // select metric from khronos_meta where metric='cp*'
    _attributeMap["table_type"] = KHRONOS_META_TABLE_TYPE;
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$metric"])json"));
    _attributeMap["condition"] = ParseJson(string(R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "LIKE",
            "params" :
            [
                "$metric",
                "cp*"
            ]
        }
    ]
})json"));

    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    TablePtr outputTable;
    bool eof;
    ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(1, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "metric", {"cpu"}));
}

void KhronosScanKernelTest::prepareDataPointScanAttributeMap() {
    prepareAttributeMap();
    // select value + 1 as data, tags['host'] as taghost, tags['app'], $ts from cpu
    // where tags['host'] = literal_or("10.10.10.1|10.10.10.2")
    // and tags['host'] = "10.10.10.1"
    // and tags['app'] = "ha3"
    // and ts > 1546272000 and ts <= 1546272004
    _attributeMap["table_type"] = KHRONOS_DATA_POINT_TABLE_TYPE;
    _attributeMap["table_name"] = string("cpu");
    _attributeMap["index_infos"] = ParseJson(string(R"json({
    "$value": {
        "name": "value",
        "type": "khronos_value"
    },
    "$ts" : {
        "name" : "ts",
        "type" : "khronos_timestamp"
    }
})json"));
    _attributeMap["output_fields"] = ParseJson(string(R"json([
    "$data",
    "$taghost",
    "$tags['app']",
    "$ts"
])json"));
    _attributeMap["output_field_exprs"] = ParseJson(string(R"json({
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
    }
})json"));

    _attributeMap["condition"] = ParseJson(string(R"json({
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
                "$ts",
                1546272000
            ],
            "type": "OTHER"
        },
        {
            "op": "<=",
            "params": [
                "$ts",
                1546272004
            ],
            "type": "OTHER"
        }
    ],
    "type": "OTHER"
})json"));
}

TEST_F(KhronosScanKernelTest, testDataPointNormal_Simple)
{
    prepareDataPointScanAttributeMap();

    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    TablePtr outputTable;
    bool eof;
    ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn<double>(outputTable, "data", {6, 6.1, 6.2}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "taghost", {"10.10.10.1", "10.10.10.1", "10.10.10.1"}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "tags['app']", {"ha3", "ha3", "ha3"}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn<KHR_TS_TYPE>(outputTable, "ts", {1546272001, 1546272002, 1546272004}));
    ASSERT_NO_FATAL_FAILURE(checkKhronosDataScanInfo<KhronosDataPointScan>(tester,
                    [&](auto &scanInfo) {
                        ASSERT_EQ(0, scanInfo.seriesoutputcount());
                        ASSERT_LE(3, scanInfo.pointscancount());
                    }));
}

TEST_F(KhronosScanKernelTest, testDataPointNormal_WithLimit)
{
    prepareDataPointScanAttributeMap();
    _attributeMap["limit"] = 2;

    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    TablePtr outputTable;
    bool eof = false;
    ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn<double>(outputTable, "data", {6, 6.1}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "taghost", {"10.10.10.1", "10.10.10.1"}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "tags['app']", {"ha3", "ha3"}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn<KHR_TS_TYPE>(outputTable, "ts", {1546272001, 1546272002}));
    ASSERT_NO_FATAL_FAILURE(checkKhronosDataScanInfo<KhronosDataPointScan>(tester,
                    [&](auto &scanInfo) {
                        ASSERT_EQ(0, scanInfo.seriesoutputcount());
                        ASSERT_LE(2, scanInfo.pointscancount());
                    }));
}

TEST_F(KhronosScanKernelTest, testDataPointNormal_WithBatch)
{
    prepareDataPointScanAttributeMap();
    _attributeMap["batch_size"] = Any(1);

    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    {
        TablePtr outputTable;
        bool eof = true;
        ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
        ASSERT_FALSE(eof);
        ASSERT_EQ(1, outputTable->getRowCount());
        ASSERT_EQ(4, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<double>(outputTable, "data", {6}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "taghost", {"10.10.10.1"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "tags['app']", {"ha3"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<KHR_TS_TYPE>(outputTable, "ts", {1546272001}));
    }
    {
        TablePtr outputTable;
        bool eof = true;
        ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
        ASSERT_FALSE(eof);
        ASSERT_EQ(1, outputTable->getRowCount());
        ASSERT_EQ(4, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<double>(outputTable, "data", {6.1}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "taghost", {"10.10.10.1"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "tags['app']", {"ha3"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<KHR_TS_TYPE>(outputTable, "ts", {1546272002}));
    }
    {
        TablePtr outputTable;
        bool eof;
        ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
        ASSERT_FALSE(eof);
        ASSERT_EQ(1, outputTable->getRowCount());
        ASSERT_EQ(4, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<double>(outputTable, "data", {6.2}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "taghost", {"10.10.10.1"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "tags['app']", {"ha3"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn<KHR_TS_TYPE>(outputTable, "ts", {1546272004}));
    }
    {
        TablePtr outputTable;
        bool eof = false;
        ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
        ASSERT_TRUE(eof);
        ASSERT_EQ(0, outputTable->getRowCount());
        ASSERT_EQ(4, outputTable->getColumnCount());
    }
    ASSERT_NO_FATAL_FAILURE(checkKhronosDataScanInfo<KhronosDataPointScan>(tester,
                    [&](auto &scanInfo) {
                        ASSERT_EQ(0, scanInfo.seriesoutputcount());
                        ASSERT_EQ(3, scanInfo.pointscancount());
                    }));
}

void KhronosScanKernelTest::prepareDataSeriesScanAttributeMap()
{
    prepareAttributeMap();
    // select value as data, tags['host'] as taghost, tags['app'] from cpu
    // where tags['host'] = literal_or("10.10.10.1|10.10.10.2")
    // and tags['app'] = "bs"
    // and timestamp_larger_equal_than(1546272000)
    // and timestamp_less_than(1546272025)
    _attributeMap["table_type"] = KHRONOS_DATA_SERIES_TABLE_TYPE;
    _attributeMap["table_name"] = string("cpu");
    _attributeMap["index_infos"] = ParseJson(string(R"json({
    "$value": {
        "name": "value",
        "type": "khronos_series"
    }
})json"));
    _attributeMap["output_fields"] = ParseJson(string(R"json([
    "$data",
    "$taghost",
    "$tags['app']"
])json"));
    _attributeMap["output_field_exprs"] = ParseJson(string(R"json({
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
    }
})json"));

    _attributeMap["condition"] = ParseJson(string(R"json({
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
})json"));

}

TEST_F(KhronosScanKernelTest, testDataSeriesNormal_Simple) {
    prepareDataSeriesScanAttributeMap();

    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    TablePtr outputTable;
    bool eof;
    ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_EQ(3, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "taghost", {"10.10.10.1", "10.10.10.2"}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "tags['app']", {"bs", "bs"}));
    ASSERT_NO_FATAL_FAILURE(
            KhronosScanTestUtil::checkDataPointList(outputTable, "data", {
                        {{1546272009,6.0}, {1546272010, 6.0}, {1546272013, 6.0}},
                        {{1546272020,6.0}, {1546272022, 6.0}}
                    }));
    ASSERT_NO_FATAL_FAILURE(checkKhronosDataScanInfo<KhronosDataSeriesScan>(tester,
                    [&](auto &scanInfo) {
                        ASSERT_EQ(5, scanInfo.pointscancount());
                        ASSERT_EQ(2, scanInfo.seriesoutputcount());
                    }));
}

TEST_F(KhronosScanKernelTest, testDataSeriesNormal_WithLimit) {
    prepareDataSeriesScanAttributeMap();
    _attributeMap["limit"] = Any(1);

    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    TablePtr outputTable;
    bool eof = false;
    ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(3, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "taghost", {"10.10.10.1"}));
    ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "tags['app']", {"bs"}));
    ASSERT_NO_FATAL_FAILURE(
            KhronosScanTestUtil::checkDataPointList(outputTable, "data", {
                        {{1546272009,6.0}, {1546272010, 6.0}, {1546272013, 6.0}},
                    }));
    ASSERT_NO_FATAL_FAILURE(checkKhronosDataScanInfo<KhronosDataSeriesScan>(tester,
                    [&](auto &scanInfo) {
                        ASSERT_EQ(5, scanInfo.pointscancount());
                        ASSERT_EQ(1, scanInfo.seriesoutputcount());
                    }));
}

TEST_F(KhronosScanKernelTest, testDataSeriesNormal_WithBatch) {
    prepareDataSeriesScanAttributeMap();
    _attributeMap["batch_size"] = Any(1);

    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    // batch 1
    {
        TablePtr outputTable;
        bool eof = true;
        ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
        ASSERT_FALSE(eof);
        ASSERT_EQ(1, outputTable->getRowCount());
        ASSERT_EQ(3, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "taghost", {"10.10.10.1"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "tags['app']", {"bs"}));
        ASSERT_NO_FATAL_FAILURE(
                KhronosScanTestUtil::checkDataPointList(outputTable, "data", {
                            {{1546272009,6.0}, {1546272010, 6.0}, {1546272013, 6.0}}
                        }));
    }
    // batch 2
    {
        TablePtr outputTable;
        bool eof = true;
        ASSERT_NO_FATAL_FAILURE(computeKernel(tester, outputTable, eof));
        ASSERT_TRUE(eof); // important !!
        ASSERT_EQ(1, outputTable->getRowCount());
        ASSERT_EQ(3, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "taghost", {"10.10.10.2"}));
        ASSERT_NO_FATAL_FAILURE(
                checkOutputColumn(outputTable, "tags['app']", {"bs"}));
        ASSERT_NO_FATAL_FAILURE(
                KhronosScanTestUtil::checkDataPointList(outputTable, "data", {
                            {{1546272020,6.0}, {1546272022, 6.0}}
                        }));
    }
    // no batch 3
    ASSERT_NO_FATAL_FAILURE(checkKhronosDataScanInfo<KhronosDataSeriesScan>(tester,
                    [&](auto &scanInfo) {
                        ASSERT_EQ(5, scanInfo.pointscancount());
                        ASSERT_EQ(2, scanInfo.seriesoutputcount());
                    }));
}

END_HA3_NAMESPACE();
