#include "sql/framework/SqlAccessLog.h"

#include <iosfwd>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>

#include "autil/TimeUtility.h"
#include "ha3/common/ErrorDefine.h"
#include "multi_call/common/ErrorCode.h"
#include "multi_call/stream/GigStreamRpcInfo.h"
#include "unittest/unittest.h"

using namespace std;
using namespace multi_call;
using namespace autil;
using namespace isearch;

namespace sql {

class SqlAccessLogTest : public TESTBASE {
public:
    SqlAccessLogTest();
    ~SqlAccessLogTest();

public:
    void setUp();
    void tearDown();

private:
    GigStreamRpcInfoMap constructLackRpcInfoMap() const;
};

SqlAccessLogTest::SqlAccessLogTest() {}

SqlAccessLogTest::~SqlAccessLogTest() {}

void SqlAccessLogTest::setUp() {}

void SqlAccessLogTest::tearDown() {}

GigStreamRpcInfoMap SqlAccessLogTest::constructLackRpcInfoMap() const {
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

TEST_F(SqlAccessLogTest, testSimpleProcess) {
    SqlAccessLog accessLog;
    accessLog.setIp("10.0.0.1");
    accessLog.setStatusCode(1001);
    accessLog.setProcessTime(11111);
    accessLog.setQueryString("select a from phone");
    accessLog.setRowCount(10);
    accessLog.setRpcInfoMap(constructLackRpcInfoMap());

    ASSERT_EQ(string("10.0.0.1"), accessLog._info.ip);
    ASSERT_EQ(string("select a from phone"), accessLog._info.queryString);
    ASSERT_EQ(int64_t(11111), accessLog._info.processTime);
    ASSERT_EQ(1001, accessLog._info.statusCode);
    ASSERT_EQ(uint32_t(10), accessLog._info.rowCount);
    ASSERT_EQ(constructLackRpcInfoMap(), accessLog._info.rpcInfoMap);

    SqlAccessLog accessLog1;
    ASSERT_EQ(string("unknownip"), accessLog1._info.ip);
    ASSERT_EQ(string(""), accessLog1._info.queryString);
    ASSERT_EQ(int64_t(0), accessLog1._info.processTime);
    ASSERT_EQ(ERROR_NONE, accessLog1._info.statusCode);
    ASSERT_EQ(uint32_t(0), accessLog1._info.rowCount);
}

} // namespace sql
