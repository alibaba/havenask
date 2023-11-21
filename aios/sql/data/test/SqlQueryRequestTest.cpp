#include "sql/data/SqlQueryRequest.h"

#include <iosfwd>
#include <memory>
#include <string>
#include <unordered_map>

#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/test/JsonTestUtil.h"
#include "sql/common/common.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;
using namespace autil::legacy;

namespace sql {

class SqlQueryRequestTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(framework, SqlQueryRequestTest);

void SqlQueryRequestTest::setUp() {}

void SqlQueryRequestTest::tearDown() {}

TEST_F(SqlQueryRequestTest, testSimple) {
    SqlQueryRequestPtr sqlQueryRequest(new SqlQueryRequest());
    sqlQueryRequest->init("select *&&kvpair=1:1;2:2");
    ASSERT_TRUE(sqlQueryRequest->validate());
    ASSERT_EQ("select *", sqlQueryRequest->getSqlQuery());
    auto kvpairs = sqlQueryRequest->getSqlParams();
    ASSERT_EQ(2, kvpairs.size());
    ASSERT_EQ("1", kvpairs["1"]);
    ASSERT_EQ("2", kvpairs["2"]);
    ASSERT_EQ(SQL_TYPE_DQL, sqlQueryRequest->getSqlType());
    autil::DataBuffer dataBuffer;
    sqlQueryRequest->serialize(dataBuffer);
    SqlQueryRequestPtr sqlQueryRequest1(new SqlQueryRequest());
    sqlQueryRequest1->deserialize(dataBuffer);
    ASSERT_EQ("select *", sqlQueryRequest1->getSqlQuery());
    kvpairs = sqlQueryRequest1->getSqlParams();
    ASSERT_EQ(2, kvpairs.size());
    ASSERT_EQ("1", kvpairs["1"]);
    ASSERT_EQ("2", kvpairs["2"]);
    auto patternStr = sqlQueryRequest1->toPatternString(123);
    string expectPatternStr
        = "{\"kvpair\":{\"1\":\"1\",\"2\":\"2\"},\"query_hash_key\":123,\"sql_str\":\"select *\"}";
    ASSERT_NO_FATAL_FAILURE(JsonTestUtil::checkJsonStringEqual(expectPatternStr, patternStr));
}

TEST_F(SqlQueryRequestTest, testGetDefaultValue) {
    {
        SqlQueryRequestPtr sqlQueryRequest(new SqlQueryRequest());
        sqlQueryRequest->init("abcd");
        ASSERT_TRUE(sqlQueryRequest->validate());
        ASSERT_EQ("unknown", sqlQueryRequest->getDatabaseName());
        ASSERT_EQ(SQL_TYPE_UNKNOWN, sqlQueryRequest->getSqlType());
        ASSERT_EQ(SQL_SOURCE_SPEC_EMPTY, sqlQueryRequest->getSourceSpec());
    }
    {
        SqlQueryRequestPtr sqlQueryRequest(new SqlQueryRequest());
        sqlQueryRequest->init("UPDATE abcd&&kvpair=databaseName:db1;exec.source.spec:s1");
        ASSERT_TRUE(sqlQueryRequest->validate());
        ASSERT_EQ("db1", sqlQueryRequest->getDatabaseName());
        ASSERT_EQ("s1", sqlQueryRequest->getSourceSpec());
        ASSERT_EQ(SQL_TYPE_DML, sqlQueryRequest->getSqlType());
    }
}

TEST_F(SqlQueryRequestTest, isResultAllowSoftFailure) {
    {
        // compatible with `lackResultEnable`
        SqlQueryRequest request;
        request._kvPair = {{SQL_LACK_RESULT_ENABLE, "true"}};
        ASSERT_TRUE(request.isResultAllowSoftFailure(false));
        ASSERT_TRUE(request.isResultAllowSoftFailure(true));
        request._kvPair = {{SQL_LACK_RESULT_ENABLE, "false"}};
        ASSERT_FALSE(request.isResultAllowSoftFailure(false));
        ASSERT_FALSE(request.isResultAllowSoftFailure(true));
    }
    {
        SqlQueryRequest request;
        request._kvPair
            = {{SQL_LACK_RESULT_ENABLE, "false"}, {SQL_RESULT_ALLOW_SOFT_FAILURE, "true"}};
        ASSERT_TRUE(request.isResultAllowSoftFailure(false));
        ASSERT_TRUE(request.isResultAllowSoftFailure(true));
        request._kvPair
            = {{SQL_LACK_RESULT_ENABLE, "true"}, {SQL_RESULT_ALLOW_SOFT_FAILURE, "false"}};
        ASSERT_FALSE(request.isResultAllowSoftFailure(false));
        ASSERT_FALSE(request.isResultAllowSoftFailure(true));
    }
    {
        SqlQueryRequest request;
        request._kvPair = {};
        ASSERT_FALSE(request.isResultAllowSoftFailure(false));
        ASSERT_TRUE(request.isResultAllowSoftFailure(true));
    }
}

TEST_F(SqlQueryRequestTest, testInit) {
    {
        string query = "select * from a";
        SqlQueryRequest request;
        ASSERT_TRUE(request.init(query));
        ASSERT_EQ(query, request.getSqlQuery());
        ASSERT_TRUE(request.getSqlParams().empty());
    }
    {
        string query = "query= select * from a";
        SqlQueryRequest request;
        ASSERT_TRUE(request.init(query));
        ASSERT_EQ("select * from a", request.getSqlQuery());
        ASSERT_TRUE(request.getSqlParams().empty());
    }
    {
        string query = "  select * from a && kvpair= a:b";
        SqlQueryRequest request;
        ASSERT_TRUE(request.init(query));
        ASSERT_EQ("select * from a", request.getSqlQuery());
        ASSERT_FALSE(request.getSqlParams().empty());
        ASSERT_EQ("b", request.getValueWithDefault("a"));
    }
    {
        string query = " query= select * from a && kvpair= a:b";
        SqlQueryRequest request;
        ASSERT_TRUE(request.init(query));
        ASSERT_EQ("select * from a", request.getSqlQuery());
        ASSERT_FALSE(request.getSqlParams().empty());
        ASSERT_EQ("b", request.getValueWithDefault("a"));
    }
    {
        string query = " kvpair= a:b && query= select * from a ";
        SqlQueryRequest request;
        ASSERT_TRUE(request.init(query));
        ASSERT_EQ("select * from a", request.getSqlQuery());
        ASSERT_FALSE(request.getSqlParams().empty());
        ASSERT_EQ("b", request.getValueWithDefault("a"));
    }
    {
        string query = " kvpair= a:b && select * from a ";
        SqlQueryRequest request;
        ASSERT_TRUE(request.init(query));
        ASSERT_EQ("select * from a", request.getSqlQuery());
        ASSERT_FALSE(request.getSqlParams().empty());
        ASSERT_EQ("b", request.getValueWithDefault("a"));
    }
    {
        string query = " a:b && select * from a ";
        SqlQueryRequest request;
        ASSERT_FALSE(request.init(query));
    }
    {
        string query = " query= a:b && select * from a ";
        SqlQueryRequest request;
        ASSERT_FALSE(request.init(query));
    }
    {
        string query = " query= a:b && query= select * from a ";
        SqlQueryRequest request;
        ASSERT_FALSE(request.init(query));
    }
    {
        string query = "";
        SqlQueryRequest request;
        ASSERT_FALSE(request.init(query));
    }
    {
        string query = "&&kvpair=a:b";
        SqlQueryRequest request;
        ASSERT_FALSE(request.init(query));
    }
    {
        string query = "kvpair=a:b";
        SqlQueryRequest request;
        ASSERT_FALSE(request.init(query));
    }
    {
        string query = "select * from a &&kvpair=a::";
        SqlQueryRequest request;
        ASSERT_TRUE(request.init(query));
        ASSERT_FALSE(request.getSqlParams().empty());
        ASSERT_EQ(":", request.getValueWithDefault("a", "B"));
    }
    {
        string query = "select * from a &&kvpair=a::b";
        SqlQueryRequest request;
        ASSERT_TRUE(request.init(query));
        ASSERT_FALSE(request.getSqlParams().empty());
        ASSERT_EQ(":b", request.getValueWithDefault("a", "B"));
    }
    {
        string query = "select * from a &&kvpair=a:";
        SqlQueryRequest request;
        ASSERT_TRUE(request.init(query));
        ASSERT_TRUE(request.getSqlParams().empty());
        ASSERT_EQ("select * from a", request.getSqlQuery());
        ASSERT_EQ("B", request.getValueWithDefault("a", "B"));
    }
    {
        string query = "select * from a &&kvpair=a";
        SqlQueryRequest request;
        ASSERT_TRUE(request.init(query));
        ASSERT_TRUE(request.getSqlParams().empty());
        ASSERT_EQ("select * from a", request.getSqlQuery());
        ASSERT_EQ("B", request.getValueWithDefault("a", "B"));
    }
    {
        string query = "select * from a &&kvpair=";
        SqlQueryRequest request;
        ASSERT_TRUE(request.init(query));
        ASSERT_TRUE(request.getSqlParams().empty());
    }
    {
        string query = "select * from a &&kvpair";
        SqlQueryRequest request;
        ASSERT_FALSE(request.init(query));
    }
}

TEST_F(SqlQueryRequestTest, testInit2) {
    {
        string query = " select * from a ";
        SqlQueryRequest request;
        ASSERT_TRUE(request.init(query, {{"A", "B"}}));
        ASSERT_EQ("select * from a", request.getSqlQuery());
        ASSERT_TRUE(!request.getSqlParams().empty());
    }
    {
        string query = " ";
        SqlQueryRequest request;
        ASSERT_FALSE(request.init(query, {}));
    }
}

TEST_F(SqlQueryRequestTest, testGetValueWithDefault) {
    SqlQueryRequest request;
    string query = "select * from a &&kvpair= a:b; c:d; b :c;  d:e;a1:\\:a2;\\:a1:a2";
    ASSERT_TRUE(request.init(query));
    ASSERT_EQ("b", request.getValueWithDefault("a"));
    ASSERT_EQ("c", request.getValueWithDefault("b"));
    ASSERT_EQ("d", request.getValueWithDefault("c"));
    ASSERT_EQ("e", request.getValueWithDefault("d"));
    ASSERT_EQ(":a2", request.getValueWithDefault("a1"));
    ASSERT_EQ("a2", request.getValueWithDefault(":a1"));
    ASSERT_EQ("", request.getValueWithDefault("e"));
    ASSERT_EQ("ff", request.getValueWithDefault("e", "ff"));
}

} // namespace sql
