#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/framework/PrometheusTableFormatter.h>
#include <ha3/sql/ops/test/OpTestBase.h>
#include <khronos_table_interface/CommonDefine.h>
#include <khronos_table_interface/DataSeries.hpp>

using namespace std;
using namespace khronos;
using namespace testing;
using namespace matchdoc;
using namespace autil;
using namespace autil::legacy;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(sql);

class PrometheusTableFormatterTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
public:
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

    void createPrometheusDataByDataStr(const std::string &dataStr,
                                 MultiChar &PrometheusData) {
        char* buf = MultiValueCreator::createMultiValueBuffer<char>(
                dataStr.data(), dataStr.size(), &_pool);
        PrometheusData.init(buf);
    }

    TablePtr createPrometheusTableFull() {
        MatchDocAllocatorPtr allocator;
        const auto &docs = getMatchDocs(allocator, 2);
        extendMatchDocAllocator(allocator, docs, "tagk1", {string("a"), string("b")});
        extendMatchDocAllocator(allocator, docs, "tagk2", {string("A"), string("B")});
        extendMatchDocAllocator<int32_t>(allocator, docs, "tagk3", {100, 200});
        extendMatchDocAllocator(allocator, docs, "metric", {string("m1"), string("m2")});
        extendMatchDocAllocator<double>(allocator, docs, "summary", {0.1, 0.2});
        extendMatchDocAllocator<KHR_WM_TYPE>(allocator, docs, "messageWatermark", {11, 22});
        extendMatchDocAllocator<float>(allocator, docs, "integrity", {0.111, 0.222});
        MultiChar PrometheusData1;
        string dataStr1;
        createDataStr({0, 1, 2}, {0.1, 0.2, 0.3}, dataStr1);
        createPrometheusDataByDataStr(dataStr1, PrometheusData1);

        MultiChar PrometheusData2;
        string dataStr2;
        createDataStr({3, 4, 5}, {0.2, 0.4, 0.6}, dataStr2);
        createPrometheusDataByDataStr(dataStr2, PrometheusData2);

        extendMatchDocAllocator<MultiChar>(allocator, docs, "dps", {PrometheusData1, PrometheusData2});
        return TablePtr(new Table(docs, allocator));
    }

     TablePtr createPrometheusTableOnlyData() {
        MatchDocAllocatorPtr allocator;
        const auto &docs = getMatchDocs(allocator, 2);
        MultiChar PrometheusData1;
        string dataStr1;
        createDataStr({0, 1, 2}, {0.1, 0.2, 0.3}, dataStr1);
        createPrometheusDataByDataStr(dataStr1, PrometheusData1);

        MultiChar PrometheusData2;
        string dataStr2;
        createDataStr({3, 4, 5}, {0.2, 0.4, 0.6}, dataStr2);
        createPrometheusDataByDataStr(dataStr2, PrometheusData2);

        extendMatchDocAllocator<MultiChar>(allocator, docs, "dps_new", {PrometheusData1, PrometheusData2});
        return TablePtr(new Table(docs, allocator));
    }

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(table, PrometheusTableFormatterTest);

void PrometheusTableFormatterTest::setUp() {
}

void PrometheusTableFormatterTest::tearDown() {
}

TEST_F(PrometheusTableFormatterTest, testparsePrometheusFormatDesc) {
    {// empty string
        string emptyStr;
        map<string, string> fieldsMap;
        ASSERT_TRUE(PrometheusTableFormatter::parsePrometheusFormatDesc(emptyStr, fieldsMap));
        ASSERT_EQ(0, fieldsMap.size());
    }
    {
        string str = "dps#b";
        map<string, string> fieldsMap;
        ASSERT_TRUE(PrometheusTableFormatter::parsePrometheusFormatDesc(str, fieldsMap));
        ASSERT_EQ(1, fieldsMap.size());
        ASSERT_EQ("b", fieldsMap["dps"]);
    }
    {
        string str = "dps#b,c#d,metric#e";
        map<string, string> fieldsMap;
        ASSERT_TRUE(PrometheusTableFormatter::parsePrometheusFormatDesc(str, fieldsMap));
        ASSERT_EQ(2, fieldsMap.size());
        ASSERT_EQ("b", fieldsMap["dps"]);
        ASSERT_EQ("e", fieldsMap["metric"]);
    }
    {
        string str = ",dps#b,,,metric#d,";
        map<string, string> fieldsMap;
        ASSERT_TRUE(PrometheusTableFormatter::parsePrometheusFormatDesc(str, fieldsMap));
        ASSERT_EQ(2, fieldsMap.size());
        ASSERT_EQ("b", fieldsMap["dps"]);
        ASSERT_EQ("d", fieldsMap["metric"]);
    }
    {
        string str = "a#b,c#d#e";
        map<string, string> fieldsMap;
        ASSERT_FALSE(PrometheusTableFormatter::parsePrometheusFormatDesc(str, fieldsMap));
    }
    {
        string str = "a#b,c";
        map<string, string> fieldsMap;
        ASSERT_FALSE(PrometheusTableFormatter::parsePrometheusFormatDesc(str, fieldsMap));
    }
}

TEST_F(PrometheusTableFormatterTest, testConvertFieldMapNotExist) {
    TablePtr table = createPrometheusTableFull();
    PrometheusResults PrometheusResults;
    {
        map<string, string> fieldsMap;
        fieldsMap["dps"] = "not_exist";
        ErrorResult errorResult = PrometheusTableFormatter::convertTable(table, fieldsMap, PrometheusResults);
        ASSERT_TRUE(errorResult.hasError());
        ASSERT_EQ(ERROR_SQL_PROMETHEUS_TO_JSON, errorResult.getErrorCode());
        ASSERT_EQ(0, PrometheusResults.size());
    }
    {
        map<string, string> fieldsMap;
        fieldsMap["metric"] = "not_exist";
        ErrorResult errorResult = PrometheusTableFormatter::convertTable(table, fieldsMap, PrometheusResults);
        ASSERT_TRUE(errorResult.hasError());
        ASSERT_EQ(ERROR_SQL_PROMETHEUS_TO_JSON, errorResult.getErrorCode());
        ASSERT_EQ(0, PrometheusResults.size());
    }
    {
        map<string, string> fieldsMap;
        fieldsMap["summary"] = "not_exist";
        ErrorResult errorResult = PrometheusTableFormatter::convertTable(table, fieldsMap, PrometheusResults);
        ASSERT_TRUE(errorResult.hasError());
        ASSERT_EQ(ERROR_SQL_PROMETHEUS_TO_JSON, errorResult.getErrorCode());
        ASSERT_EQ(0, PrometheusResults.size());
    }
    {
        map<string, string> fieldsMap;
        fieldsMap["integrity"] = "not_exist";
        ErrorResult errorResult = PrometheusTableFormatter::convertTable(table, fieldsMap, PrometheusResults);
        ASSERT_TRUE(errorResult.hasError());
        ASSERT_EQ(ERROR_SQL_PROMETHEUS_TO_JSON, errorResult.getErrorCode());
        ASSERT_EQ(0, PrometheusResults.size());
    }
    {
        map<string, string> fieldsMap;
        fieldsMap["messageWatermark"] = "not_exist";
        ErrorResult errorResult = PrometheusTableFormatter::convertTable(table, fieldsMap, PrometheusResults);
        ASSERT_TRUE(errorResult.hasError());
        ASSERT_EQ(ERROR_SQL_PROMETHEUS_TO_JSON, errorResult.getErrorCode());
        ASSERT_EQ(0, PrometheusResults.size());
    }
}

TEST_F(PrometheusTableFormatterTest, testConvertMemoryExhausted) {
    TablePtr table = createPrometheusTableFull();
    PrometheusResults PrometheusResults;
    map<string, string> fieldsMap;
    ErrorResult errorResult = PrometheusTableFormatter::convertTable(table, fieldsMap, -1, PrometheusResults, 0);
    ASSERT_TRUE(errorResult.hasError());
    ASSERT_EQ(ERROR_SQL_PROMETHEUS_TO_JSON, errorResult.getErrorCode());
    string errorMsg = errorResult.getErrorMsg();
    ASSERT_NE(string::npos, errorMsg.find("too many output result"));
}

TEST_F(PrometheusTableFormatterTest, testConvertPrometheusFull) {
    TablePtr table = createPrometheusTableFull();
    PrometheusResults PrometheusResults;
    map<string, string> fieldsMap;
    ErrorResult errorResult = PrometheusTableFormatter::convertTable(table, fieldsMap, PrometheusResults);
    ASSERT_TRUE(!errorResult.hasError());
    ASSERT_EQ(2, PrometheusResults.size());

    PrometheusResult PrometheusResult0 = PrometheusResults[0];
    map<string, string> &tags0 = PrometheusResult0.metric;
    ASSERT_EQ(4, tags0.size());
    ASSERT_EQ(string("m1"), tags0["__name__"]);
    ASSERT_EQ(string("a"), tags0["tagk1"]);
    ASSERT_EQ(string("A"), tags0["tagk2"]);
    ASSERT_EQ(string("100"), tags0["tagk3"]);
    auto const &values0 = PrometheusResult0.values;
    ASSERT_EQ(3, values0.size());
    ASSERT_DOUBLE_EQ(0.1, stod(AnyCast<string>(values0[0][1])));
    ASSERT_DOUBLE_EQ(0.2, stod(AnyCast<string>(values0[1][1])));
    ASSERT_DOUBLE_EQ(0.3, stod(AnyCast<string>(values0[2][1])));

    ASSERT_FLOAT_EQ(0.1, PrometheusResult0.summary);
    ASSERT_EQ(11, PrometheusResult0.messageWatermark);
    ASSERT_FLOAT_EQ(0.111, PrometheusResult0.integrity);

    PrometheusResult PrometheusResult1 = PrometheusResults[1];
    map<string, string> &tags1 = PrometheusResult1.metric;
    ASSERT_EQ(4, tags1.size());
    ASSERT_EQ(string("m2"), tags1["__name__"]);
    ASSERT_EQ(string("b"), tags1["tagk1"]);
    ASSERT_EQ(string("B"), tags1["tagk2"]);
    ASSERT_EQ(string("200"), tags1["tagk3"]);
    auto const &values1 = PrometheusResult1.values;
    ASSERT_EQ(3, values1.size());
    ASSERT_DOUBLE_EQ(0.2, stod(AnyCast<string>(values1[0][1])));
    ASSERT_DOUBLE_EQ(0.4, stod(AnyCast<string>(values1[1][1])));
    ASSERT_DOUBLE_EQ(0.6, stod(AnyCast<string>(values1[2][1])));
    ASSERT_DOUBLE_EQ(0.2, PrometheusResult1.summary);
    ASSERT_EQ(22, PrometheusResult1.messageWatermark);
    ASSERT_FLOAT_EQ(0.222, PrometheusResult1.integrity);

}

TEST_F(PrometheusTableFormatterTest, testConvertPrometheusOnlyData) {
    TablePtr table = createPrometheusTableOnlyData();
    PrometheusResults PrometheusResults;
    map<string, string> fieldsMap;
    fieldsMap["dps"] = "dps_new";
    ErrorResult errorResult = PrometheusTableFormatter::convertTable(table, fieldsMap, PrometheusResults);
    ASSERT_TRUE(!errorResult.hasError());
    ASSERT_EQ(2, PrometheusResults.size());
    PrometheusResult PrometheusResult0 = PrometheusResults[0];
    map<string, string> &tags0 = PrometheusResult0.metric;
    ASSERT_EQ(0, tags0.size());
    auto const &values0 = PrometheusResult0.values;
    ASSERT_EQ(3, values0.size());
    ASSERT_DOUBLE_EQ(0.1, stod(AnyCast<string>(values0[0][1])));
    ASSERT_DOUBLE_EQ(0.2, stod(AnyCast<string>(values0[1][1])));
    ASSERT_DOUBLE_EQ(0.3, stod(AnyCast<string>(values0[2][1])));

    ASSERT_FLOAT_EQ(-1, PrometheusResult0.summary);
    ASSERT_EQ(-1, PrometheusResult0.messageWatermark);
    ASSERT_FLOAT_EQ(-1, PrometheusResult0.integrity);

    PrometheusResult PrometheusResult1 = PrometheusResults[1];
    map<string, string> &tags1 = PrometheusResult1.metric;
    ASSERT_EQ(0, tags1.size());
    auto const &values1 = PrometheusResult1.values;
    ASSERT_EQ(3, values1.size());
    ASSERT_DOUBLE_EQ(0.2, stod(AnyCast<string>(values1[0][1])));
    ASSERT_DOUBLE_EQ(0.4, stod(AnyCast<string>(values1[1][1])));
    ASSERT_DOUBLE_EQ(0.6, stod(AnyCast<string>(values1[2][1])));
    ASSERT_FLOAT_EQ(-1, PrometheusResult1.summary);
    ASSERT_EQ(-1, PrometheusResult1.messageWatermark);
    ASSERT_FLOAT_EQ(-1, PrometheusResult1.integrity);
}

TEST_F(PrometheusTableFormatterTest, testConvertPrometheusWithIntegrity) {
    TablePtr table = createPrometheusTableOnlyData();
    PrometheusResults PrometheusResults;
    map<string, string> fieldsMap;
    fieldsMap["dps"] = "dps_new";
    ErrorResult errorResult = PrometheusTableFormatter::convertTable(table, fieldsMap, 0.9, PrometheusResults);
    ASSERT_TRUE(!errorResult.hasError());
    ASSERT_EQ(2, PrometheusResults.size());
    PrometheusResult PrometheusResult0 = PrometheusResults[0];
    map<string, string> &tags0 = PrometheusResult0.metric;
    ASSERT_EQ(0, tags0.size());
    auto const &values0 = PrometheusResult0.values;
    ASSERT_EQ(3, values0.size());
    ASSERT_DOUBLE_EQ(0.1, stod(AnyCast<string>(values0[0][1])));
    ASSERT_DOUBLE_EQ(0.2, stod(AnyCast<string>(values0[1][1])));
    ASSERT_DOUBLE_EQ(0.3, stod(AnyCast<string>(values0[2][1])));

    ASSERT_FLOAT_EQ(-1, PrometheusResult0.summary);
    ASSERT_EQ(-1, PrometheusResult0.messageWatermark);
    ASSERT_FLOAT_EQ(0.9, PrometheusResult0.integrity);

    PrometheusResult PrometheusResult1 = PrometheusResults[1];
    map<string, string> &tags1 = PrometheusResult1.metric;
    ASSERT_EQ(0, tags1.size());
    auto const &values1 = PrometheusResult1.values;
    ASSERT_EQ(3, values1.size());
    ASSERT_DOUBLE_EQ(0.2, stod(AnyCast<string>(values1[0][1])));
    ASSERT_DOUBLE_EQ(0.4, stod(AnyCast<string>(values1[1][1])));
    ASSERT_DOUBLE_EQ(0.6, stod(AnyCast<string>(values1[2][1])));
    ASSERT_FLOAT_EQ(-1, PrometheusResult1.summary);
    ASSERT_EQ(-1, PrometheusResult1.messageWatermark);
    ASSERT_FLOAT_EQ(0.9, PrometheusResult1.integrity);
}

TEST_F(PrometheusTableFormatterTest, testConvertPrometheusEmpty) {
    TablePtr table = createPrometheusTableOnlyData();
    PrometheusResults PrometheusResults;
    map<string, string> fieldsMap;
    ErrorResult errorResult = PrometheusTableFormatter::convertTable(table, fieldsMap, PrometheusResults);
    ASSERT_TRUE(errorResult.hasError());
    ASSERT_EQ(ERROR_SQL_PROMETHEUS_TO_JSON, errorResult.getErrorCode());
    ASSERT_EQ(0, PrometheusResults.size());
}

END_HA3_NAMESPACE(sql);
