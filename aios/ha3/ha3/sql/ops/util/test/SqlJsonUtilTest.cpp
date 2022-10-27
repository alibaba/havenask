#include <ha3/test/test.h>
#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/util/SqlJsonUtil.h>
#include <unittest/unittest.h>

using namespace std;
BEGIN_HA3_NAMESPACE(sql);

class SqlJsonUtilTest : public TESTBASE {
public:
    SqlJsonUtilTest();
    ~SqlJsonUtilTest();
};

SqlJsonUtilTest::SqlJsonUtilTest() {
}

SqlJsonUtilTest::~SqlJsonUtilTest() {
}

TEST_F(SqlJsonUtilTest, testColumnUtil) {
    {
        autil_rapidjson::SimpleDocument simpleDoc;
        simpleDoc.Parse("{\"name\":\"$aaa\"}");
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_TRUE(SqlJsonUtil::isColumn(simpleDoc["name"]));
        ASSERT_EQ("aaa", SqlJsonUtil::getColumnName(simpleDoc["name"]));
    }
    {
        autil_rapidjson::SimpleDocument simpleDoc;
        simpleDoc.Parse("{\"name\":\"$\"}");
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_FALSE(SqlJsonUtil::isColumn(simpleDoc["name"]));
    }
    {
        autil_rapidjson::SimpleDocument simpleDoc;
        simpleDoc.Parse("{\"name\":\"aaa\"}");
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_FALSE(SqlJsonUtil::isColumn(simpleDoc["name"]));
    }
    {
        autil_rapidjson::SimpleDocument simpleDoc;
        simpleDoc.Parse("{\"name\":\"[]\"}");
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_FALSE(SqlJsonUtil::isColumn(simpleDoc["name"]));
    }
}

END_HA3_NAMESPACE(sql);
