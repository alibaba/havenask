#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/khronosScan/KhronosCommon.h>
#include <ha3/sql/ops/khronosScan/KhronosScanKernel.h>
#include <ha3/sql/ops/khronosScan/KhronosDataScan.h>
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
using ::testing::_;

BEGIN_HA3_NAMESPACE(sql);

template <class T, class M> M get_member_type(M T:: *);
#define GET_TYPE_OF(mem) decltype(get_member_type(mem))

class MockKhronosDataScan : public KhronosDataScan {
public:
    MockKhronosDataScan(const KhronosScanHints &scanHints = {})
        : KhronosDataScan(scanHints, nullptr)
    {}
public:
    MOCK_METHOD2(doBatchScanImpl, bool(TablePtr &table, bool &eof));
protected:
    MOCK_METHOD0(parseQueryParamsFromCondition, bool());
    MOCK_METHOD1(extractUsedField, bool(OutputFieldsVisitor &visitor));
};

class KhronosDataScanTest : public OpTestBase {
public:
    KhronosDataScanTest()
    {
        _tableName = "simple_table";
        _needBuildIndex = false;
        _needExprResource = false;
    }
    void setUp() override;
protected:
    ScanInitParam createBaseScanInitParam();
protected:
    MemoryPoolResource _memoryPoolResource;
    IndexInfoMapType _indexInfos;
};

class KhronosDataScanTestWithIndex : public KhronosDataScanTest {
public:
    KhronosDataScanTestWithIndex() {
        _needBuildIndex = true;
    }
public:
    void setUp() override {
        KhronosDataScanTest::setUp();
    }
    void tearDown() override {
        clearResource();
        _indexApp.reset();
        KhronosDataScanTest::tearDown();
    }
    void prepareIndex() override {
        ASSERT_TRUE(_tableBuilder.build(_testPath, {}));
        _indexApp = _tableBuilder._indexApp;
    }
protected:
    KhronosTestTableBuilder _tableBuilder;
};

void KhronosDataScanTest::setUp() {
    string indexInfosStr = R"json({
    "$value": {
        "name": "value",
        "type": "khronos_value"
    },
    "$timestamp": {
        "name": "timestamp",
        "type": "khronos_timestamp"
    }
})json";
    FromJsonString(_indexInfos, indexInfosStr);
}

ScanInitParam KhronosDataScanTest::createBaseScanInitParam() {
    ScanInitParam param;
    param.catalogName = _tableName;
    param.tableType = KHRONOS_DATA_POINT_TABLE_TYPE;
    param.bizResource = _sqlBizResource.get();
    param.queryResource = _sqlQueryResource.get();
    param.indexInfos = _indexInfos;
    param.memoryPoolResource = &_memoryPoolResource;
    return param;
}

TEST_F(KhronosDataScanTestWithIndex, init_Error_ParseQueryParamsFromCondition) {
    ScanInitParam param = createBaseScanInitParam();

    MockKhronosDataScan scan;
    EXPECT_CALL(scan, parseQueryParamsFromCondition())
        .Times(1)
        .WillOnce(Return(false));

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.init(param));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "parse query params from condition failed", traces);
}


TEST_F(KhronosDataScanTestWithIndex, init_Error_ParseUsedFields) {
    ScanInitParam param = createBaseScanInitParam();

    param.conditionJson = R"json({})json";
    param.outputExprsJson = R"json({})json";
    param.outputFields = {
    };

    MockKhronosDataScan scan;
    EXPECT_CALL(scan, parseQueryParamsFromCondition())
        .Times(1)
        .WillOnce(Return(true));
    EXPECT_CALL(scan, extractUsedField(_))
        .Times(1)
        .WillOnce(Return(false));

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.init(param));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "parse used fields failed", traces);
}

TEST_F(KhronosDataScanTestWithIndex, init_Error_NoBizResource) {
    ScanInitParam param = createBaseScanInitParam();
    param.bizResource = nullptr;

    MockKhronosDataScan scan;

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.init(param));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "get sql biz resource failed.", traces);
}

TEST_F(KhronosDataScanTestWithIndex, init_Error_LookupFailed) {
    ScanInitParam param = createBaseScanInitParam();
    param.outputExprsJson = R"json({})json";
    param.outputFields = {
    };

    MockKhronosDataScan scan;
    scan._valueColName = "nonexistfield";
    EXPECT_CALL(scan, parseQueryParamsFromCondition())
        .Times(1)
        .WillOnce(Return(true));
    EXPECT_CALL(scan, extractUsedField(_))
        .Times(1)
        .WillOnce(Return(true));

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.init(param));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "table reader lookup failed, ret=-1", traces);
}

TEST_F(KhronosDataScanTestWithIndex, init_Success) {
    ScanInitParam param = createBaseScanInitParam();
    param.outputExprsJson = R"json({})json";

    MockKhronosDataScan scan;
    EXPECT_CALL(scan, parseQueryParamsFromCondition())
        .Times(1)
        .WillOnce(Return(true));
    EXPECT_CALL(scan, extractUsedField(_))
        .Times(1)
        .WillOnce(Return(true));
    scan._valueColName = "$value";
    scan._tsRange.begin = 1546272004;

    navi::NaviLoggerProvider provider("TRACE3");
    ASSERT_TRUE(scan.init(param));
    ASSERT_NE(nullptr, scan._rowIter);
}

TEST_F(KhronosDataScanTest, parseUsedFields_Error_InvalidOutputExprsJson) {
    MockKhronosDataScan scan;
    scan._pool = &_pool;
    scan._kernelPool = _memPoolResource.getPool();
    scan._indexInfos = _indexInfos;
    scan._outputFields = {
        "$value",
        "$taghost",
    };
    scan._outputExprsJson = R"json(aaa)json";

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.parseUsedFields());
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "parse output exprs error", traces);
}

TEST_F(KhronosDataScanTest, parseUsedFields_Error_VisitError) {
    MockKhronosDataScan scan;
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
        ],
        "type": "OTHER"
    }
})json";

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.parseUsedFields());
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "visitor visit failed", traces);
}

TEST_F(KhronosDataScanTest, parseUsedFields_Error_ExtractUsedFieldFailed) {
    MockKhronosDataScan scan;
    scan._pool = &_pool;
    scan._kernelPool = _memPoolResource.getPool();
    scan._indexInfos = _indexInfos;
    scan._outputFields = {
    };
    scan._outputExprsJson = R"json({})json";

    EXPECT_CALL(scan, extractUsedField(_))
        .Times(1)
        .WillOnce(Return(false));

    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scan.parseUsedFields());
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "extract used field failed", traces);
}

TEST_F(KhronosDataScanTest, parseUsedFields_Success) {
    MockKhronosDataScan scan;
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

    GET_TYPE_OF(&OutputFieldsVisitor::_usedFieldsColumnSet) usedFieldsColumnSet;
    GET_TYPE_OF(&OutputFieldsVisitor::_usedFieldsItemSet) usedFieldsItemSet;
    EXPECT_CALL(scan, extractUsedField(_))
        .Times(1)
        .WillOnce(Invoke([&] (OutputFieldsVisitor &visitor)
                         {
                             usedFieldsItemSet = visitor._usedFieldsItemSet;
                             usedFieldsColumnSet = visitor._usedFieldsColumnSet;
                             return true;
                         }));

    ASSERT_TRUE(scan.parseUsedFields());
    ASSERT_NE(usedFieldsColumnSet.end(), usedFieldsColumnSet.find("$value"));
    ASSERT_NE(usedFieldsItemSet.end(), usedFieldsItemSet.find({"$tags", "host"}));
}

TEST_F(KhronosDataScanTest, lookupParamAsString_Success) {
    MockKhronosDataScan scan;
    scan._valueColName = "$avg_value";
    scan._tsRange.begin = 100;
    scan._tsRange.end = 101;
    scan._tagKVInfos.emplace_back(ConstString("zone"),
                                  (SearchInterface::TagvMatchInfo)
                                  {ConstString("good|bad"), SearchInterface::LITERAL_OR});
    scan._tagKVInfos.emplace_back(ConstString("app"),
                                  (SearchInterface::TagvMatchInfo)
                                  {ConstString("ha3"), SearchInterface::LITERAL});
    string output = scan.lookupParamsAsString();
    ASSERT_EQ("catalog=[], metric=[], tsRange=[100, 101), tagKVInfos={(zone,1,[good|bad]) (app,0,[ha3])}, valueName=[$avg_value]",
              output);
}

TEST_F(KhronosDataScanTest, updateTagkColumnValueCache_Error_TagvNotFound) {
    MockKhronosDataScan scan;
    scan._pool = &_pool;
    KhronosDataScan::TagkColumnPackMap tagkColumnPackMap;
    {
        string seriesKey = string("cpu") + KHR_SERIES_KEY_METRIC_SPLITER + "host";

        navi::NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(
                scan.updateTagkColumnValueCache(seriesKey, tagkColumnPackMap));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "tagv not found for tagk=[host], skLen=[8], curPos=[9], sk=",
                          traces);
    }
}

TEST_F(KhronosDataScanTest, updateTagkColumnValueCache_Success) {
    MockKhronosDataScan scan;
    scan._pool = &_pool;

    {
        KhronosDataScan::TagkColumnPackMap tagkColumnPackMap = {
            {"host", {nullptr}},
            {"instance", {nullptr}}
        };
        // no metric sep, treat as no tags
        string seriesKey = "";
        ASSERT_TRUE(
                scan.updateTagkColumnValueCache(seriesKey, tagkColumnPackMap));
        ASSERT_NO_FATAL_FAILURE(
                checkValueEQ("", tagkColumnPackMap["host"].columnValueCache));
        ASSERT_NO_FATAL_FAILURE(
                checkValueEQ("", tagkColumnPackMap["instance"].columnValueCache));
    }
    {
        KhronosDataScan::TagkColumnPackMap tagkColumnPackMap = {
            {"host", {nullptr}},
            {"instance", {nullptr}}
        };
        // no tags fields
        string seriesKey = string("cpu") + KHR_SERIES_KEY_METRIC_SPLITER;
        ASSERT_TRUE(
                scan.updateTagkColumnValueCache(seriesKey, tagkColumnPackMap));
        ASSERT_NO_FATAL_FAILURE(
                checkValueEQ("", tagkColumnPackMap["host"].columnValueCache));
        ASSERT_NO_FATAL_FAILURE(
                checkValueEQ("", tagkColumnPackMap["instance"].columnValueCache));
    }
    {
        string seriesKey = string("cpu") + KHR_SERIES_KEY_METRIC_SPLITER;
        seriesKey += string("host") + KHR_TAGKV_SPLITER + "";

        KhronosDataScan::TagkColumnPackMap tagkColumnPackMap = {
            {"host", {nullptr}},
            {"instance", {nullptr}}
        };
        ASSERT_TRUE(
                scan.updateTagkColumnValueCache(seriesKey, tagkColumnPackMap));
        ASSERT_NO_FATAL_FAILURE(
                checkValueEQ("", tagkColumnPackMap["host"].columnValueCache));
        ASSERT_NO_FATAL_FAILURE(
                checkValueEQ("", tagkColumnPackMap["instance"].columnValueCache));
    }
    {
        string seriesKey = string("cpu") + KHR_SERIES_KEY_METRIC_SPLITER;
        seriesKey += string("host") + KHR_TAGKV_SPLITER + "" + KHR_SERIES_KEY_TAGS_SPLITER;

        KhronosDataScan::TagkColumnPackMap tagkColumnPackMap = {
            {"host", {nullptr}},
            {"instance", {nullptr}}
        };
        ASSERT_TRUE(
                scan.updateTagkColumnValueCache(seriesKey, tagkColumnPackMap));
        ASSERT_NO_FATAL_FAILURE(
                checkValueEQ("", tagkColumnPackMap["host"].columnValueCache));
        ASSERT_NO_FATAL_FAILURE(
                checkValueEQ("", tagkColumnPackMap["instance"].columnValueCache));
    }
    {
        string seriesKey = string("cpu") + KHR_SERIES_KEY_METRIC_SPLITER;
        seriesKey += string("host") + KHR_TAGKV_SPLITER + "10.10.10.1";

        KhronosDataScan::TagkColumnPackMap tagkColumnPackMap = {
            {"host", {nullptr}},
            {"instance", {nullptr}}
        };
        ASSERT_TRUE(
                scan.updateTagkColumnValueCache(seriesKey, tagkColumnPackMap));
        ASSERT_NO_FATAL_FAILURE(
                checkValueEQ("10.10.10.1", tagkColumnPackMap["host"].columnValueCache));
        ASSERT_NO_FATAL_FAILURE(
                checkValueEQ("", tagkColumnPackMap["instance"].columnValueCache));
    }
    {
        string seriesKey = string("cpu") + KHR_SERIES_KEY_METRIC_SPLITER;
        seriesKey += string("host") + KHR_TAGKV_SPLITER + "10.10.10.1";
        seriesKey += KHR_SERIES_KEY_TAGS_SPLITER; // no use

        KhronosDataScan::TagkColumnPackMap tagkColumnPackMap = {
            {"host", {nullptr}},
            {"instance", {nullptr}}
        };
        ASSERT_TRUE(
                scan.updateTagkColumnValueCache(seriesKey, tagkColumnPackMap));
        ASSERT_NO_FATAL_FAILURE(
                checkValueEQ("10.10.10.1", tagkColumnPackMap["host"].columnValueCache));
        ASSERT_NO_FATAL_FAILURE(
                checkValueEQ("", tagkColumnPackMap["instance"].columnValueCache));
    }
    {
        string seriesKey = string("cpu") + KHR_SERIES_KEY_METRIC_SPLITER;
        seriesKey += string("host") + KHR_TAGKV_SPLITER + "10.10.10.1";
        seriesKey += KHR_SERIES_KEY_TAGS_SPLITER;
        seriesKey += string("app") + KHR_TAGKV_SPLITER + "ha3";

        KhronosDataScan::TagkColumnPackMap tagkColumnPackMap = {
            {"host", {nullptr}},
            {"instance", {nullptr}}
        };
        ASSERT_TRUE(
                scan.updateTagkColumnValueCache(seriesKey, tagkColumnPackMap));
        ASSERT_NO_FATAL_FAILURE(
                checkValueEQ("10.10.10.1", tagkColumnPackMap["host"].columnValueCache));
        ASSERT_NO_FATAL_FAILURE(
                checkValueEQ("", tagkColumnPackMap["instance"].columnValueCache));
    }
    {
        string seriesKey = string("cpu") + KHR_SERIES_KEY_METRIC_SPLITER;
        seriesKey += string("host") + KHR_TAGKV_SPLITER + "10.10.10.1";
        seriesKey += KHR_SERIES_KEY_TAGS_SPLITER;
        seriesKey += string("app") + KHR_TAGKV_SPLITER + "ha3";

        KhronosDataScan::TagkColumnPackMap tagkColumnPackMap = {
            {"host", {nullptr}},
            {"app", {nullptr}}
        };
        ASSERT_TRUE(
                scan.updateTagkColumnValueCache(seriesKey, tagkColumnPackMap));
        ASSERT_NO_FATAL_FAILURE(
                checkValueEQ("10.10.10.1", tagkColumnPackMap["host"].columnValueCache));
        ASSERT_NO_FATAL_FAILURE(
                checkValueEQ("ha3", tagkColumnPackMap["app"].columnValueCache));
    }
}

TEST_F(KhronosDataScanTest, updateTagkColumnValueCache_perf) {
    MockKhronosDataScan scan;
    scan._pool = &_pool;


    {
        string seriesKey = string("cpu") + KHR_SERIES_KEY_METRIC_SPLITER;
        seriesKey += string("host") + KHR_TAGKV_SPLITER + "10.10.10.1";
        seriesKey += KHR_SERIES_KEY_TAGS_SPLITER;
        seriesKey += string("app") + KHR_TAGKV_SPLITER + "ha3";
        seriesKey += KHR_SERIES_KEY_TAGS_SPLITER;
        seriesKey += string("table") + KHR_TAGKV_SPLITER + "good";
        seriesKey += KHR_SERIES_KEY_TAGS_SPLITER;
        seriesKey += string("instance") + KHR_TAGKV_SPLITER + "ea120";

        KhronosDataScan::TagkColumnPackMap tagkColumnPackMap = {
            {"host", {nullptr}},
            {"app", {nullptr}}
        };
        int64_t rt = 0;
        {
            RTMetricTimer timer(rt);
            for (size_t i = 0; i < 140000; ++i) {
                scan.updateTagkColumnValueCache(seriesKey, tagkColumnPackMap);
            }
        }
        cout << "~~~time use : " << rt << endl;

        ASSERT_NO_FATAL_FAILURE(
                checkValueEQ("10.10.10.1", tagkColumnPackMap["host"].columnValueCache));
        ASSERT_NO_FATAL_FAILURE(
                checkValueEQ("ha3", tagkColumnPackMap["app"].columnValueCache));
    }
}

END_HA3_NAMESPACE();
