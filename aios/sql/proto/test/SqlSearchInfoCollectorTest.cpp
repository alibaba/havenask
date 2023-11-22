#include "sql/proto/SqlSearchInfoCollector.h"

#include "autil/DataBuffer.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/log/test/NaviLoggerProviderTestUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;
using namespace navi;

namespace sql {

class SqlSearchInfoCollectorTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(sql, SqlSearchInfoCollectorTest);

void SqlSearchInfoCollectorTest::setUp() {}

void SqlSearchInfoCollectorTest::tearDown() {}

TEST_F(SqlSearchInfoCollectorTest, testSerializeDeserialize_Success_Empty) {
    DataBuffer dataBuffer;
    {
        SqlSearchInfoCollector collector;
        collector.serialize(dataBuffer);
        ASSERT_LT(0, dataBuffer.getDataLen());
    }
    {
        SqlSearchInfoCollector collector;
        collector.deserialize(dataBuffer);
        ASSERT_EQ(0, dataBuffer.getDataLen());
    }
}

TEST_F(SqlSearchInfoCollectorTest, testSerializeDeserialize_Success_Simple) {
    DataBuffer dataBuffer;
    {
        SqlSearchInfoCollector collector;
        ScanInfo scanInfo;
        scanInfo.set_tablename("aaaa");
        collector.overwriteScanInfo(scanInfo);

        collector.serialize(dataBuffer);
        ASSERT_LT(0, dataBuffer.getDataLen());
    }
    {
        SqlSearchInfoCollector collector;
        collector.deserialize(dataBuffer);
        ASSERT_EQ(0, dataBuffer.getDataLen());

        auto &sqlSearchInfo = collector._sqlSearchInfo;
        ASSERT_EQ(1, sqlSearchInfo.scaninfos_size());
        auto *scanInfo = sqlSearchInfo.mutable_scaninfos(0);
        ASSERT_EQ("aaaa", scanInfo->tablename());
    }
}

TEST_F(SqlSearchInfoCollectorTest, testMerge_Error_InvalidChecksum) {
    DataBuffer dataBuffer;
    dataBuffer.write((size_t)0);

    SqlSearchInfoCollector collector;
    navi::NaviLoggerProvider provider("ERROR");
    collector.deserialize(dataBuffer);
    ASSERT_NO_FATAL_FAILURE(
        NaviLoggerProviderTestUtil::checkTraceCount(1, "invalid checksum", provider));
}

TEST_F(SqlSearchInfoCollectorTest, testMerge_Error_DeserializeCollector) {
    DataBuffer dataBuffer;
    dataBuffer.write(SqlSearchInfoCollector::SQL_SEARCHINFO_CHECKSUM_BEGIN);
    dataBuffer.write((size_t)1);
    dataBuffer.write(SqlSearchInfoCollector::SQL_SEARCHINFO_CHECKSUM_END);

    SqlSearchInfoCollector collector;
    NaviLoggerProvider provider("ERROR");
    collector.deserialize(dataBuffer);
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
        1, "sql searchinfo parse from array failed", provider));
}

TEST_F(SqlSearchInfoCollectorTest, testFinal_WithMergeAndOverwrite) {
    SqlSearchInfoCollector qrsCollector;
    {
        ScanInfo scanInfo;
        DegradedInfo degradedInfo;
        scanInfo.set_tablename("aaaa");
        scanInfo.set_hashkey(1);
        qrsCollector.overwriteScanInfo(scanInfo);
        qrsCollector.overwriteScanInfo(scanInfo);
        qrsCollector.overwriteScanInfo(scanInfo);
        degradedInfo.add_degradederrorcodes(IO_ERROR);
        qrsCollector.addDegradedInfo(degradedInfo);

        scanInfo.set_tablename("bbbb");
        scanInfo.set_hashkey(2);
        qrsCollector.overwriteScanInfo(scanInfo);
        qrsCollector.overwriteScanInfo(scanInfo);
        qrsCollector.overwriteScanInfo(scanInfo);
    }
    SqlSearchInfoCollector searcherCollector;
    DataBuffer dataBuffer;
    {
        ScanInfo scanInfo;
        DegradedInfo degradedInfo;
        scanInfo.set_tablename("cccc");
        scanInfo.set_hashkey(1);
        searcherCollector.overwriteScanInfo(scanInfo);
        degradedInfo.add_degradederrorcodes(RPC_ERROR);
        degradedInfo.add_degradederrorcodes(IO_ERROR);
        degradedInfo.add_degradederrorcodes(INCOMPLETE_DATA_ERROR);
        searcherCollector.addDegradedInfo(degradedInfo);

        searcherCollector.serialize(dataBuffer);
        ASSERT_LT(0, dataBuffer.getDataLen());
    }
    searcherCollector.deserialize(dataBuffer);
    qrsCollector.mergeSqlSearchInfo(std::move(searcherCollector._sqlSearchInfo));
    const auto &sqlSearchInfo = qrsCollector._sqlSearchInfo;
    ASSERT_EQ(2, sqlSearchInfo.scaninfos_size());

    auto &first = sqlSearchInfo.scaninfos(0);
    ASSERT_EQ(1, first.hashkey());
    ASSERT_EQ("aaaa", first.tablename());
    ASSERT_EQ(3, first.mergecount());

    auto &second = sqlSearchInfo.scaninfos(1);
    ASSERT_EQ(2, second.hashkey());
    ASSERT_EQ("bbbb", second.tablename());
    ASSERT_EQ(2, second.mergecount());

    ASSERT_EQ(3, sqlSearchInfo.degradedinfos(0).degradederrorcodes_size());
    ASSERT_EQ(IO_ERROR, sqlSearchInfo.degradedinfos(0).degradederrorcodes(0));
    ASSERT_EQ(RPC_ERROR, sqlSearchInfo.degradedinfos(0).degradederrorcodes(1));
    ASSERT_EQ(INCOMPLETE_DATA_ERROR, sqlSearchInfo.degradedinfos(0).degradederrorcodes(2));
}

} // namespace sql
