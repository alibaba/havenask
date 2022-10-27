#include <unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/SqlAccessLog.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);

class SqlAccessLogTest : public TESTBASE {
public:
    SqlAccessLogTest();
    ~SqlAccessLogTest();
public:
    void setUp();
    void tearDown();
    
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, SqlAccessLogTest);


SqlAccessLogTest::SqlAccessLogTest() { 
}

SqlAccessLogTest::~SqlAccessLogTest() { 
}

void SqlAccessLogTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void SqlAccessLogTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(SqlAccessLogTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    SqlAccessLog accessLog;
    accessLog.setIp("10.0.0.1");
    accessLog.setStatusCode(1001);
    accessLog.setProcessTime(11111);
    accessLog.setQueryString("select a from phone");
    accessLog.setRowCount(10);
    
    ASSERT_EQ(string("10.0.0.1"), accessLog._ip);
    ASSERT_EQ(string("select a from phone"), accessLog._queryString);
    ASSERT_EQ(int64_t(11111), accessLog._processTime);
    ASSERT_EQ(1001, accessLog._statusCode);
    ASSERT_EQ(uint32_t(10), accessLog._rowCount);

    SqlAccessLog accessLog1;
    ASSERT_EQ(string("unknownip"), accessLog1._ip);
    ASSERT_EQ(string(""), accessLog1._queryString);
    ASSERT_EQ(int64_t(0), accessLog1._processTime);
    ASSERT_EQ(ERROR_NONE, accessLog1._statusCode);
    ASSERT_EQ(uint32_t(0), accessLog1._rowCount);
}

END_HA3_NAMESPACE(common);

