#include "sql/framework/SqlAccessLogFormatHelper.h"

#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include "multi_call/common/ErrorCode.h"
#include "multi_call/stream/GigStreamRpcInfo.h"
#include "sql/framework/SqlAccessLog.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace multi_call;

namespace sql {

class SqlAccessLogFormatHelperTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    GigStreamRpcInfoMap constructLackRpcInfoMap() const;
};

void SqlAccessLogFormatHelperTest::setUp() {}

void SqlAccessLogFormatHelperTest::tearDown() {}

GigStreamRpcInfoMap SqlAccessLogFormatHelperTest::constructLackRpcInfoMap() const {
    GigStreamRpcInfoMap rpcInfoMap;
    GigStreamRpcInfoVec rpcInfoVec(10);
    for (size_t i = 0; i < 10; ++i) {
        rpcInfoVec[i].sendStatus = GigStreamRpcStatus::GSRS_EOF;
        rpcInfoVec[i].receiveStatus = GigStreamRpcStatus::GSRS_CANCELLED;
    }
    rpcInfoVec[2].sendStatus = GigStreamRpcStatus::GSRS_EOF;
    rpcInfoVec[2].receiveStatus = GigStreamRpcStatus::GSRS_EOF;
    rpcInfoMap.emplace(std::make_pair("source_biz", "test_biz"), rpcInfoVec);
    return rpcInfoMap;
}

TEST_F(SqlAccessLogFormatHelperTest, testParseCoveredPercent) {
    {
        GigStreamRpcInfoMap rpcInfoMap;
        ASSERT_DOUBLE_EQ(1.0, SqlAccessLogFormatHelper::parseCoveredPercent(rpcInfoMap));
    }
    {
        GigStreamRpcInfoMap rpcInfoMap = constructLackRpcInfoMap();
        ASSERT_DOUBLE_EQ(0.1, SqlAccessLogFormatHelper::parseCoveredPercent(rpcInfoMap));
    }
}

TEST_F(SqlAccessLogFormatHelperTest, testParseSoftFailure) {
    {
        SqlAccessLog accessLog;
        accessLog.setRpcInfoMap({});
        auto *scanInfo = accessLog._info.searchInfo.add_scaninfos();
        scanInfo->set_degradeddocscount(0);
        ASSERT_FALSE(SqlAccessLogFormatHelper::parseSoftFailure(accessLog));
    }
    {
        SqlAccessLog accessLog;
        accessLog.setRpcInfoMap(constructLackRpcInfoMap());
        ASSERT_TRUE(SqlAccessLogFormatHelper::parseSoftFailure(accessLog));
    }

    {
        SqlAccessLog accessLog;
        auto *degradedInfo = accessLog._info.searchInfo.add_degradedinfos();
        degradedInfo->add_degradederrorcodes(IO_ERROR);
        ASSERT_TRUE(SqlAccessLogFormatHelper::parseSoftFailure(accessLog));
    }
}

TEST_F(SqlAccessLogFormatHelperTest, testParseSoftFailureCodes) {
    {
        SqlAccessLog accessLog;
        ASSERT_EQ(SqlAccessLogFormatHelper::parseSoftFailureCodes(accessLog).size(), 0);
    }
    {
        SqlAccessLog accessLog;
        auto *degradedInfo = accessLog._info.searchInfo.add_degradedinfos();
        degradedInfo->add_degradederrorcodes(IO_ERROR);
        degradedInfo->add_degradederrorcodes(RPC_ERROR);
        EXPECT_THAT(SqlAccessLogFormatHelper::parseSoftFailureCodes(accessLog),
                    ElementsAre(IO_ERROR, RPC_ERROR));
    }
}

} // namespace sql
