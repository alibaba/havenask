#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/framework/TsdbTableFormatter.h>
#include <ha3/sql/ops/test/OpTestBase.h>
#include <khronos_table_interface/CommonDefine.h>
#include <khronos_table_interface/DataSeries.hpp>

using namespace std;
using namespace khronos;
using namespace testing;
using namespace matchdoc;
using namespace autil;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(sql);

class TsdbTableFormatterTest : public OpTestBase {
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

    void createTsdbDataByDataStr(const std::string &dataStr,
                                 MultiChar &tsdbData) {
        char* buf = MultiValueCreator::createMultiValueBuffer<char>(
                dataStr.data(), dataStr.size(), &_pool);
        tsdbData.init(buf);
    }

    TablePtr createTsdbTableFull() {
        MatchDocAllocatorPtr allocator;
        const auto &docs = getMatchDocs(allocator, 2);
        extendMatchDocAllocator(allocator, docs, "tagk1", {string("a"), string("b")});
        extendMatchDocAllocator(allocator, docs, "tagk2", {string("A"), string("B")});
        extendMatchDocAllocator<int32_t>(allocator, docs, "tagk3", {100, 200});
        extendMatchDocAllocator(allocator, docs, "metric", {string("m1"), string("m2")});
        extendMatchDocAllocator<double>(allocator, docs, "summary", {0.1, 0.2});
        extendMatchDocAllocator<KHR_WM_TYPE>(allocator, docs, "messageWatermark", {11, 22});
        extendMatchDocAllocator<float>(allocator, docs, "integrity", {0.111, 0.222});
        MultiChar tsdbData1;
        string dataStr1;
        createDataStr({0, 1, 2}, {0.1, 0.2, 0.3}, dataStr1);
        createTsdbDataByDataStr(dataStr1, tsdbData1);

        MultiChar tsdbData2;
        string dataStr2;
        createDataStr({3, 4, 5}, {0.2, 0.4, 0.6}, dataStr2);
        createTsdbDataByDataStr(dataStr2, tsdbData2);

        extendMatchDocAllocator<MultiChar>(allocator, docs, "dps", {tsdbData1, tsdbData2});
        return TablePtr(new Table(docs, allocator));
    }

     TablePtr createTsdbTableOnlyData() {
        MatchDocAllocatorPtr allocator;
        const auto &docs = getMatchDocs(allocator, 2);
        MultiChar tsdbData1;
        string dataStr1;
        createDataStr({0, 1, 2}, {0.1, 0.2, 0.3}, dataStr1);
        createTsdbDataByDataStr(dataStr1, tsdbData1);

        MultiChar tsdbData2;
        string dataStr2;
        createDataStr({3, 4, 5}, {0.2, 0.4, 0.6}, dataStr2);
        createTsdbDataByDataStr(dataStr2, tsdbData2);

        extendMatchDocAllocator<MultiChar>(allocator, docs, "dps_new", {tsdbData1, tsdbData2});
        return TablePtr(new Table(docs, allocator));
    }

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(table, TsdbTableFormatterTest);

void TsdbTableFormatterTest::setUp() {
}

void TsdbTableFormatterTest::tearDown() {
}

TEST_F(TsdbTableFormatterTest, testparseTsdbFormatDesc) {
    {// empty string
        string emptyStr;
        map<string, string> fieldsMap;
        ASSERT_TRUE(TsdbTableFormatter::parseTsdbFormatDesc(emptyStr, fieldsMap));
        ASSERT_EQ(0, fieldsMap.size());
    }
    {
        string str = "dps#b";
        map<string, string> fieldsMap;
        ASSERT_TRUE(TsdbTableFormatter::parseTsdbFormatDesc(str, fieldsMap));
        ASSERT_EQ(1, fieldsMap.size());
        ASSERT_EQ("b", fieldsMap["dps"]);
    }
    {
        string str = "dps#b,c#d,metric#e";
        map<string, string> fieldsMap;
        ASSERT_TRUE(TsdbTableFormatter::parseTsdbFormatDesc(str, fieldsMap));
        ASSERT_EQ(2, fieldsMap.size());
        ASSERT_EQ("b", fieldsMap["dps"]);
        ASSERT_EQ("e", fieldsMap["metric"]);
    }
    {
        string str = ",dps#b,,,metric#d,";
        map<string, string> fieldsMap;
        ASSERT_TRUE(TsdbTableFormatter::parseTsdbFormatDesc(str, fieldsMap));
        ASSERT_EQ(2, fieldsMap.size());
        ASSERT_EQ("b", fieldsMap["dps"]);
        ASSERT_EQ("d", fieldsMap["metric"]);
    }
    {
        string str = "a#b,c#d#e";
        map<string, string> fieldsMap;
        ASSERT_FALSE(TsdbTableFormatter::parseTsdbFormatDesc(str, fieldsMap));
    }
    {
        string str = "a#b,c";
        map<string, string> fieldsMap;
        ASSERT_FALSE(TsdbTableFormatter::parseTsdbFormatDesc(str, fieldsMap));
    }
}

TEST_F(TsdbTableFormatterTest, testConvertFieldMapNotExist) {
    TablePtr table = createTsdbTableFull();
    TsdbResults tsdbResults;
    {
        map<string, string> fieldsMap;
        fieldsMap["dps"] = "not_exist";
        ErrorResult errorResult = TsdbTableFormatter::convertTable(table, fieldsMap, tsdbResults);
        ASSERT_TRUE(errorResult.hasError());
        ASSERT_EQ(ERROR_SQL_TSDB_TO_JSON, errorResult.getErrorCode());
        ASSERT_EQ(0, tsdbResults.size());
    }
    {
        map<string, string> fieldsMap;
        fieldsMap["metric"] = "not_exist";
        ErrorResult errorResult = TsdbTableFormatter::convertTable(table, fieldsMap, tsdbResults);
        ASSERT_TRUE(errorResult.hasError());
        ASSERT_EQ(ERROR_SQL_TSDB_TO_JSON, errorResult.getErrorCode());
        ASSERT_EQ(0, tsdbResults.size());
    }
    {
        map<string, string> fieldsMap;
        fieldsMap["summary"] = "not_exist";
        ErrorResult errorResult = TsdbTableFormatter::convertTable(table, fieldsMap, tsdbResults);
        ASSERT_TRUE(errorResult.hasError());
        ASSERT_EQ(ERROR_SQL_TSDB_TO_JSON, errorResult.getErrorCode());
        ASSERT_EQ(0, tsdbResults.size());
    }
    {
        map<string, string> fieldsMap;
        fieldsMap["integrity"] = "not_exist";
        ErrorResult errorResult = TsdbTableFormatter::convertTable(table, fieldsMap, tsdbResults);
        ASSERT_TRUE(errorResult.hasError());
        ASSERT_EQ(ERROR_SQL_TSDB_TO_JSON, errorResult.getErrorCode());
        ASSERT_EQ(0, tsdbResults.size());
    }
    {
        map<string, string> fieldsMap;
        fieldsMap["messageWatermark"] = "not_exist";
        ErrorResult errorResult = TsdbTableFormatter::convertTable(table, fieldsMap, tsdbResults);
        ASSERT_TRUE(errorResult.hasError());
        ASSERT_EQ(ERROR_SQL_TSDB_TO_JSON, errorResult.getErrorCode());
        ASSERT_EQ(0, tsdbResults.size());
    }
}

TEST_F(TsdbTableFormatterTest, testConvertMemoryExhausted) {
    TablePtr table = createTsdbTableFull();
    TsdbResults tsdbResults;
    map<string, string> fieldsMap;
    ErrorResult errorResult = TsdbTableFormatter::convertTable(table, fieldsMap, -1, tsdbResults, 0);
    ASSERT_TRUE(errorResult.hasError());
    ASSERT_EQ(ERROR_SQL_TSDB_TO_JSON, errorResult.getErrorCode());
    string errorMsg = errorResult.getErrorMsg();
    ASSERT_NE(string::npos, errorMsg.find("too many output result"));
}

TEST_F(TsdbTableFormatterTest, testConvertTsdbFull) {
    TablePtr table = createTsdbTableFull();
    TsdbResults tsdbResults;
    map<string, string> fieldsMap;
    ErrorResult errorResult = TsdbTableFormatter::convertTable(table, fieldsMap, tsdbResults);
    ASSERT_TRUE(!errorResult.hasError());
    ASSERT_EQ(2, tsdbResults.size());

    TsdbResult tsdbResult0 = tsdbResults[0];
    map<string, string> &tags0 = tsdbResult0.tags;
    ASSERT_EQ(3, tags0.size());
    ASSERT_EQ(string("a"), tags0["tagk1"]);
    ASSERT_EQ(string("A"), tags0["tagk2"]);
    ASSERT_EQ(string("100"), tags0["tagk3"]);
    map<string, float> &dps0 = tsdbResult0.dps;
    ASSERT_EQ(3, dps0.size());
    ASSERT_FLOAT_EQ(0.1, dps0["0"]);
    ASSERT_FLOAT_EQ(0.2, dps0["1"]);
    ASSERT_FLOAT_EQ(0.3, dps0["2"]);

    ASSERT_EQ("m1", tsdbResult0.metric);
    ASSERT_FLOAT_EQ(0.1, tsdbResult0.summary);
    ASSERT_EQ(11, tsdbResult0.messageWatermark);
    ASSERT_FLOAT_EQ(0.111, tsdbResult0.integrity);

    TsdbResult tsdbResult1 = tsdbResults[1];
    map<string, string> &tags1 = tsdbResult1.tags;
    ASSERT_EQ(3, tags1.size());
    ASSERT_EQ(string("b"), tags1["tagk1"]);
    ASSERT_EQ(string("B"), tags1["tagk2"]);
    ASSERT_EQ(string("200"), tags1["tagk3"]);
    map<string, float> &dps1 = tsdbResult1.dps;
    ASSERT_EQ(3, dps1.size());
    ASSERT_FLOAT_EQ(0.2, dps1["3"]);
    ASSERT_FLOAT_EQ(0.4, dps1["4"]);
    ASSERT_FLOAT_EQ(0.6, dps1["5"]);
    ASSERT_EQ("m2", tsdbResult1.metric);
    ASSERT_FLOAT_EQ(0.2, tsdbResult1.summary);
    ASSERT_EQ(22, tsdbResult1.messageWatermark);
    ASSERT_FLOAT_EQ(0.222, tsdbResult1.integrity);

}

TEST_F(TsdbTableFormatterTest, testConvertTsdbOnlyData) {
    TablePtr table = createTsdbTableOnlyData();
    TsdbResults tsdbResults;
    map<string, string> fieldsMap;
    fieldsMap["dps"] = "dps_new";
    ErrorResult errorResult = TsdbTableFormatter::convertTable(table, fieldsMap, tsdbResults);
    ASSERT_TRUE(!errorResult.hasError());
    ASSERT_EQ(2, tsdbResults.size());
    TsdbResult tsdbResult0 = tsdbResults[0];
    map<string, string> &tags0 = tsdbResult0.tags;
    ASSERT_EQ(0, tags0.size());
    map<string, float> &dps0 = tsdbResult0.dps;
    ASSERT_EQ(3, dps0.size());
    ASSERT_FLOAT_EQ(0.1, dps0["0"]);
    ASSERT_FLOAT_EQ(0.2, dps0["1"]);
    ASSERT_FLOAT_EQ(0.3, dps0["2"]);

    ASSERT_EQ("", tsdbResult0.metric);
    ASSERT_FLOAT_EQ(-1, tsdbResult0.summary);
    ASSERT_EQ(-1, tsdbResult0.messageWatermark);
    ASSERT_FLOAT_EQ(-1, tsdbResult0.integrity);

    TsdbResult tsdbResult1 = tsdbResults[1];
    map<string, string> &tags1 = tsdbResult1.tags;
    ASSERT_EQ(0, tags1.size());
    map<string, float> &dps1 = tsdbResult1.dps;
    ASSERT_EQ(3, dps1.size());
    ASSERT_FLOAT_EQ(0.2, dps1["3"]);
    ASSERT_FLOAT_EQ(0.4, dps1["4"]);
    ASSERT_FLOAT_EQ(0.6, dps1["5"]);
    ASSERT_EQ("", tsdbResult1.metric);
    ASSERT_FLOAT_EQ(-1, tsdbResult1.summary);
    ASSERT_EQ(-1, tsdbResult1.messageWatermark);
    ASSERT_FLOAT_EQ(-1, tsdbResult1.integrity);
}

TEST_F(TsdbTableFormatterTest, testConvertTsdbWithIntegrity) {
    TablePtr table = createTsdbTableOnlyData();
    TsdbResults tsdbResults;
    map<string, string> fieldsMap;
    fieldsMap["dps"] = "dps_new";
    ErrorResult errorResult = TsdbTableFormatter::convertTable(table, fieldsMap, 0.9, tsdbResults);
    ASSERT_TRUE(!errorResult.hasError());
    ASSERT_EQ(2, tsdbResults.size());
    TsdbResult tsdbResult0 = tsdbResults[0];
    map<string, string> &tags0 = tsdbResult0.tags;
    ASSERT_EQ(0, tags0.size());
    map<string, float> &dps0 = tsdbResult0.dps;
    ASSERT_EQ(3, dps0.size());
    ASSERT_FLOAT_EQ(0.1, dps0["0"]);
    ASSERT_FLOAT_EQ(0.2, dps0["1"]);
    ASSERT_FLOAT_EQ(0.3, dps0["2"]);

    ASSERT_EQ("", tsdbResult0.metric);
    ASSERT_FLOAT_EQ(-1, tsdbResult0.summary);
    ASSERT_EQ(-1, tsdbResult0.messageWatermark);
    ASSERT_FLOAT_EQ(0.9, tsdbResult0.integrity);

    TsdbResult tsdbResult1 = tsdbResults[1];
    map<string, string> &tags1 = tsdbResult1.tags;
    ASSERT_EQ(0, tags1.size());
    map<string, float> &dps1 = tsdbResult1.dps;
    ASSERT_EQ(3, dps1.size());
    ASSERT_FLOAT_EQ(0.2, dps1["3"]);
    ASSERT_FLOAT_EQ(0.4, dps1["4"]);
    ASSERT_FLOAT_EQ(0.6, dps1["5"]);
    ASSERT_EQ("", tsdbResult1.metric);
    ASSERT_FLOAT_EQ(-1, tsdbResult1.summary);
    ASSERT_EQ(-1, tsdbResult1.messageWatermark);
    ASSERT_FLOAT_EQ(0.9, tsdbResult1.integrity);
}

TEST_F(TsdbTableFormatterTest, testConvertTsdbEmpty) {
    TablePtr table = createTsdbTableOnlyData();
    TsdbResults tsdbResults;
    map<string, string> fieldsMap;
    ErrorResult errorResult = TsdbTableFormatter::convertTable(table, fieldsMap, tsdbResults);
    ASSERT_TRUE(errorResult.hasError());
    ASSERT_EQ(ERROR_SQL_TSDB_TO_JSON, errorResult.getErrorCode());
    ASSERT_EQ(0, tsdbResults.size());
}

END_HA3_NAMESPACE(sql);
