#include "sql/ops/condition/SqlJsonUtil.h"

#include <iosfwd>

#include "autil/legacy/RapidJsonCommon.h"
#include "rapidjson/document.h"
#include "unittest/unittest.h"

using namespace std;
namespace sql {

class SqlJsonUtilTest : public TESTBASE {
public:
    SqlJsonUtilTest();
    ~SqlJsonUtilTest();
};

SqlJsonUtilTest::SqlJsonUtilTest() {}

SqlJsonUtilTest::~SqlJsonUtilTest() {}

TEST_F(SqlJsonUtilTest, testColumnUtil) {
    {
        autil::SimpleDocument simpleDoc;
        simpleDoc.Parse("{\"name\":\"$aaa\"}");
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_TRUE(SqlJsonUtil::isColumn(simpleDoc["name"]));
        ASSERT_EQ("aaa", SqlJsonUtil::getColumnName(simpleDoc["name"]));
    }
    {
        autil::SimpleDocument simpleDoc;
        simpleDoc.Parse("{\"name\":\"$\"}");
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_FALSE(SqlJsonUtil::isColumn(simpleDoc["name"]));
    }
    {
        autil::SimpleDocument simpleDoc;
        simpleDoc.Parse("{\"name\":\"aaa\"}");
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_FALSE(SqlJsonUtil::isColumn(simpleDoc["name"]));
    }
    {
        autil::SimpleDocument simpleDoc;
        simpleDoc.Parse("{\"name\":\"[]\"}");
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_FALSE(SqlJsonUtil::isColumn(simpleDoc["name"]));
    }
}

} // namespace sql
