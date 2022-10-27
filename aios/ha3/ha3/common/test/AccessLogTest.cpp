#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/AccessLog.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);

class AccessLogTest : public TESTBASE {
public:
    AccessLogTest();
    ~AccessLogTest();
public:
    void setUp();
    void tearDown();

protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, AccessLogTest);


AccessLogTest::AccessLogTest() {
}

AccessLogTest::~AccessLogTest() {
}

void AccessLogTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void AccessLogTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(AccessLogTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");
    AccessLog accessLog;
    accessLog.setIp("10.0.0.1");
    accessLog.setStatusCode(1001);
    accessLog.setProcessTime(11111);
    accessLog.setQueryString("config=daogou");
    accessLog.setTotalHits(10);
    PhaseOneSearchInfo searchInfo(1,2,3,4,5,6,7,8,9);
    accessLog.setPhaseOneSearchInfo(searchInfo);

    ASSERT_EQ(string("10.0.0.1"), accessLog._ip);
    ASSERT_EQ(string("config=daogou"), accessLog._queryString);
    ASSERT_EQ(int64_t(11111), accessLog._processTime);
    ASSERT_EQ(1001, accessLog._statusCode);
    ASSERT_EQ(uint32_t(10), accessLog._totalHits);
    ASSERT_EQ(string(""), accessLog._userData);
    ASSERT_EQ(string(""), accessLog._traceId);
    AccessLog accessLog1;
    ASSERT_EQ(string("unknownip"), accessLog1._ip);
    ASSERT_EQ(string(""), accessLog1._queryString);
    ASSERT_EQ(int64_t(0), accessLog1._processTime);
    ASSERT_EQ(ERROR_NONE, accessLog1._statusCode);
    ASSERT_EQ(uint32_t(0), accessLog1._totalHits);
    AccessLog accessLog2;

    AccessLog accesslog3;
    accesslog3.setUserData("testtokenstrXXXXXXXXXXXXXX");
    ASSERT_EQ(string("testtokenstrXXXXXXXXXXXXXX"), accesslog3._userData);
    accesslog3.setTraceId("aaaa");
    ASSERT_EQ(string("aaaa"), accesslog3._traceId);

    AccessLog accesslog4;
    accesslog4.setUserData("");
    ASSERT_EQ(string(""), accesslog4._userData);

}

END_HA3_NAMESPACE(common);
