#include "build_service/proto/ErrorCollector.h"

#include "build_service/test/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace proto {

class ErrorCollectorTest : public BUILD_SERVICE_TESTBASE
{
};

TEST_F(ErrorCollectorTest, testAddErrorInfo)
{
    ErrorCollector collector;
    collector.setMaxErrorCount(2);
    collector.addErrorInfo(SERVICE_ERROR_CONFIG, "reader config", BS_RETRY);
    collector.addErrorInfo(READER_ERROR_INIT_METRICS, "init metrics", BS_RETRY);

    ASSERT_EQ(size_t(2), collector._errorInfos.size());
    EXPECT_EQ(READER_ERROR_INIT_METRICS, collector._errorInfos[0].errorcode());
    EXPECT_EQ("init metrics", collector._errorInfos[0].errormsg());
    EXPECT_EQ(SERVICE_ERROR_CONFIG, collector._errorInfos[1].errorcode());
    EXPECT_EQ("reader config", collector._errorInfos[1].errormsg());

    collector.addErrorInfo(BUILDER_ERROR_BUILD_FILEIO, "build file io", BS_RETRY);
    ASSERT_EQ(size_t(2), collector._errorInfos.size());
    EXPECT_EQ(BUILDER_ERROR_BUILD_FILEIO, collector._errorInfos[0].errorcode());
    EXPECT_EQ("build file io", collector._errorInfos[0].errormsg());
    EXPECT_EQ(READER_ERROR_INIT_METRICS, collector._errorInfos[1].errorcode());
    EXPECT_EQ("init metrics", collector._errorInfos[1].errormsg());

    collector.addErrorInfo(BUILDER_ERROR_DUMP_FILEIO, "dump file io", BS_RETRY);
    ASSERT_EQ(size_t(2), collector._errorInfos.size());
    EXPECT_EQ(BUILDER_ERROR_DUMP_FILEIO, collector._errorInfos[0].errorcode());
    EXPECT_EQ("dump file io", collector._errorInfos[0].errormsg());
    EXPECT_EQ(BUILDER_ERROR_BUILD_FILEIO, collector._errorInfos[1].errorcode());
    EXPECT_EQ("build file io", collector._errorInfos[1].errormsg());

    collector.addErrorInfo(BUILDER_ERROR_BUILD_FILEIO, string(4096, 'a'), BS_RETRY);
    EXPECT_EQ(string(2048, 'a'), collector._errorInfos[0].errormsg());

    std::vector<ErrorInfo> errorInfos;
    errorInfos.resize(5);
    collector.addErrorInfos(errorInfos);
    ASSERT_EQ(size_t(2), collector._errorInfos.size());
}

}} // namespace build_service::proto
