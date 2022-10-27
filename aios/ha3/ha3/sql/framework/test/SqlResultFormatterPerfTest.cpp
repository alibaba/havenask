#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/framework/SqlResultFormatter.h>
#include <ha3/sql/ops/test/OpTestBase.h>

using namespace std;
using namespace testing;
using namespace matchdoc;
using namespace autil;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(service);
BEGIN_HA3_NAMESPACE(sql);
static const int32_t maxCount = 3;
class SqlResultFormatterPerfTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
private:
    template<typename T>
    TablePtr createSimpleTable(size_t rowCount, size_t colCount) {
        MatchDocAllocatorPtr allocator;
        const auto &docs = getMatchDocs(allocator, rowCount);
        vector<T> colValues(rowCount, (T)0);
        for (size_t i = 0; i < colCount; i ++) {
            string colName = string("col") + StringUtil::toString(i);
            extendMatchDocAllocator(allocator, docs, colName, colValues);
        }
        return TablePtr(new Table(docs, allocator));
    }

    template<typename T>
    TablePtr createSimpleMultiTable(size_t rowCount, size_t colCount) {
        MatchDocAllocatorPtr allocator;
        const auto &docs = getMatchDocs(allocator, rowCount);
        vector<vector<T> > colValues(rowCount, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        for (size_t i = 0; i < colCount; i ++) {
            string colName = string("col") + StringUtil::toString(i);
            extendMultiValueMatchDocAllocator(allocator, docs, colName, colValues);
        }
        return TablePtr(new Table(docs, allocator));
    }

    TablePtr createStringTable(size_t rowCount, size_t colCount) {
        MatchDocAllocatorPtr allocator;
        const auto &docs = getMatchDocs(allocator, rowCount);
        vector<string> colValues(rowCount, "a");
        for (size_t i = 0; i < colCount; i ++) {
            string colName = string("col") + StringUtil::toString(i);
            extendMatchDocAllocator(allocator, docs, colName, colValues);
        }
        return TablePtr(new Table(docs, allocator));
    }

    TablePtr createMultiStringTable(size_t rowCount, size_t colCount) {
        MatchDocAllocatorPtr allocator;
        const auto &docs = getMatchDocs(allocator, rowCount);
        vector<vector<string> > colValues(rowCount, {"a", "b", "c", "d",
                        "e", "f", "g", "h", "i", "j"});
        for (size_t i = 0; i < colCount; i ++) {
            string colName = string("col") + StringUtil::toString(i);
            extendMultiValueMatchDocAllocator(allocator, docs, colName, colValues);
        }
        return TablePtr(new Table(docs, allocator));
    }


    template<typename T>
    void testSinglePerf(const std::string &valueType) {
        printf("begin perf: [%s]\n", valueType.c_str());
        QrsSessionSqlResult sqlResult;
        vector<size_t> rowCountVec = {1, 10, 100, 500, 1000, 2000};
        vector<size_t> colCountVec = {1, 10, 40, 80, 100};
        for (auto colCount : colCountVec) {
            for (auto rowCount : rowCountVec) {
                int32_t count = maxCount;
                double totalTimeJson = 0.0;
                uint32_t jsonResultSize = 0u;
                while (count--) {
                    sqlResult.table = createSimpleTable<T>(rowCount, colCount);
                    double t1 = autil::TimeUtility::currentTime() / 1000.;
                    SqlResultFormatter::formatJson(sqlResult, nullptr);
                    double t2 = autil::TimeUtility::currentTime() / 1000.;
                    totalTimeJson += t2 - t1;
                    jsonResultSize = sqlResult.resultStr.size();
                }

                int32_t count2 = maxCount;
                autil::mem_pool::Pool pool;
                double totalTimeFB = 0.0;
                uint32_t fbResultSize = 0u;
                while (count2--) {
                    sqlResult.table = createSimpleTable<T>(rowCount, colCount);
                    double t1 = autil::TimeUtility::currentTime() / 1000.;
                    SqlResultFormatter::formatFlatbuffers(sqlResult, NULL, &pool);
                    double t2 = autil::TimeUtility::currentTime() / 1000.;
                    totalTimeFB += t2 - t1;
                    fbResultSize = sqlResult.resultStr.size();
                }
                printf("%-6d * %6d: json[%lfms %dbyte] flatbuffer[%lfms %dbyte]\n",
                       (int32_t)rowCount, (int32_t)colCount,
                       totalTimeJson/maxCount, jsonResultSize,
                       totalTimeFB/maxCount, fbResultSize);
            }
        }
        printf("end perf: ==============================================\n");
    }

    template<typename T>
    void testMultiPerf(const std::string &valueType) {
        printf("begin perf: [multi %s]\n", valueType.c_str());
        QrsSessionSqlResult sqlResult;
        vector<size_t> rowCountVec = {1, 10, 100, 500, 1000, 2000};
        vector<size_t> colCountVec = {1, 10, 40, 80, 100};
        for (auto colCount : colCountVec) {
            for (auto rowCount : rowCountVec) {
                int32_t count = maxCount;
                double totalTimeJson = 0.0;
                uint32_t jsonResultSize = 0u;
                while (count--) {
                    sqlResult.table = createSimpleMultiTable<T>(rowCount, colCount);
                    double t1 = autil::TimeUtility::currentTime() / 1000.;
                    SqlResultFormatter::formatJson(sqlResult, nullptr);
                    double t2 = autil::TimeUtility::currentTime() / 1000.;
                    totalTimeJson += t2 - t1;
                    jsonResultSize = sqlResult.resultStr.size();
                }

                int32_t count2 = maxCount;
                autil::mem_pool::Pool pool;
                double totalTimeFB = 0.0;
                uint32_t fbResultSize = 0u;
                while (count2--) {
                    sqlResult.table = createSimpleMultiTable<T>(rowCount, colCount);
                    double t1 = autil::TimeUtility::currentTime() / 1000.;
                    SqlResultFormatter::formatFlatbuffers(sqlResult, NULL, &pool);
                    double t2 = autil::TimeUtility::currentTime() / 1000.;
                    totalTimeFB += t2 - t1;
                    fbResultSize = sqlResult.resultStr.size();
                }
                printf("%-6d * %6d: json[%lfms %dbyte] flatbuffer[%lfms %dbyte]\n",
                       (int32_t)rowCount, (int32_t)colCount,
                       totalTimeJson/maxCount, jsonResultSize,
                       totalTimeFB/maxCount, fbResultSize);
            }
        }
        printf("end perf: ==============================================\n");
    }

    void testStringPerf() {
        printf("begin perf: [string]\n");
        QrsSessionSqlResult sqlResult;
        vector<size_t> rowCountVec = {1, 10, 100, 500, 1000, 2000};
        vector<size_t> colCountVec = {1, 10, 40, 80, 100};
        for (auto colCount : colCountVec) {
            for (auto rowCount : rowCountVec) {
                int32_t count = maxCount;
                double totalTimeJson = 0.0;
                uint32_t jsonResultSize = 0u;
                while (count--) {
                    sqlResult.table = createStringTable(rowCount, colCount);
                    double t1 = autil::TimeUtility::currentTime() / 1000.;
                    SqlResultFormatter::formatJson(sqlResult, nullptr);
                    double t2 = autil::TimeUtility::currentTime() / 1000.;
                    totalTimeJson += t2 - t1;
                    jsonResultSize = sqlResult.resultStr.size();
                }

                int32_t count2 = maxCount;
                autil::mem_pool::Pool pool;
                double totalTimeFB = 0.0;
                uint32_t fbResultSize = 0u;
                while (count2--) {
                    sqlResult.table = createStringTable(rowCount, colCount);
                    double t1 = autil::TimeUtility::currentTime() / 1000.;
                    SqlResultFormatter::formatFlatbuffers(sqlResult, NULL, &pool);
                    double t2 = autil::TimeUtility::currentTime() / 1000.;
                    totalTimeFB += t2 - t1;
                    fbResultSize = sqlResult.resultStr.size();
                }
                printf("%-6d * %6d: json[%lfms %dbyte] flatbuffer[%lfms %dbyte]\n",
                       (int32_t)rowCount, (int32_t)colCount,
                       totalTimeJson/maxCount, jsonResultSize,
                       totalTimeFB/maxCount, fbResultSize);
            }
        }
        printf("end perf: ==============================================\n");
    }

    void testMultiStringPerf() {
        printf("begin perf: [multi string]\n");
        QrsSessionSqlResult sqlResult;
        vector<size_t> rowCountVec = {1, 10, 100, 500, 1000, 2000};
        vector<size_t> colCountVec = {1, 10, 40, 80, 100};
        for (auto colCount : colCountVec) {
            for (auto rowCount : rowCountVec) {
                int32_t count = maxCount;
                double totalTimeJson = 0.0;
                uint32_t jsonResultSize = 0u;
                while (count--) {
                    sqlResult.table = createMultiStringTable(rowCount, colCount);
                    double t1 = autil::TimeUtility::currentTime() / 1000.;
                    SqlResultFormatter::formatJson(sqlResult, nullptr);
                    double t2 = autil::TimeUtility::currentTime() / 1000.;
                    totalTimeJson += t2 - t1;
                    jsonResultSize = sqlResult.resultStr.size();
                }

                int32_t count2 = maxCount;
                autil::mem_pool::Pool pool;
                double totalTimeFB = 0.0;
                uint32_t fbResultSize = 0u;
                while (count2--) {
                    sqlResult.table = createMultiStringTable(rowCount, colCount);
                    double t1 = autil::TimeUtility::currentTime() / 1000.;
                    SqlResultFormatter::formatFlatbuffers(sqlResult, NULL, &pool);
                    double t2 = autil::TimeUtility::currentTime() / 1000.;
                    totalTimeFB += t2 - t1;
                    fbResultSize = sqlResult.resultStr.size();
                }
                printf("%-6d * %6d: json[%lfms %dbyte] flatbuffer[%lfms %dbyte]\n",
                       (int32_t)rowCount, (int32_t)colCount,
                       totalTimeJson/maxCount, jsonResultSize,
                       totalTimeFB/maxCount, fbResultSize);
            }
        }
        printf("end perf: ==============================================\n");
    }

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(table, SqlResultFormatterPerfTest);

void SqlResultFormatterPerfTest::setUp() {
}

void SqlResultFormatterPerfTest::tearDown() {
}

TEST_F(SqlResultFormatterPerfTest, DISABLED_testSingleValuePerf) {
    testSinglePerf<uint8_t>("uint8_t");
    testSinglePerf<uint16_t>("uint16_t");
    testSinglePerf<uint32_t>("uint32_t");
    testSinglePerf<uint64_t>("uint64_t");
    testSinglePerf<int8_t>("int8_t");
    testSinglePerf<int16_t>("int16_t");
    testSinglePerf<int32_t>("int32_t");
    testSinglePerf<int64_t>("int64_t");
    testSinglePerf<float>("float");
    testSinglePerf<double>("double");
}

TEST_F(SqlResultFormatterPerfTest, DISABLED_testMultiValuePerf) {
    testMultiPerf<uint8_t>("uint8_t");
    testMultiPerf<uint16_t>("uint16_t");
    testMultiPerf<uint32_t>("uint32_t");
    testMultiPerf<uint64_t>("uint64_t");
    testMultiPerf<int8_t>("int8_t");
    testMultiPerf<int16_t>("int16_t");
    testMultiPerf<int32_t>("int32_t");
    testMultiPerf<int64_t>("int64_t");
    testMultiPerf<float>("float");
    testMultiPerf<double>("double");
}

TEST_F(SqlResultFormatterPerfTest, DISABLED_testStringPerf) {
    testStringPerf();
    testMultiStringPerf();
}


END_HA3_NAMESPACE(sql);
