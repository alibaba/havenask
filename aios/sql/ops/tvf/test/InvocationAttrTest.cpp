#include "sql/ops/tvf/InvocationAttr.h"

#include <iosfwd>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "sql/ops/test/OpTestBase.h"
#include "unittest/unittest.h"

using namespace std;

namespace sql {

class InvocationAttrTest : public OpTestBase {
public:
    void setUp();
    void tearDown();

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(sql, InvocationAttrTest);

void InvocationAttrTest::setUp() {}

void InvocationAttrTest::tearDown() {}

TEST_F(InvocationAttrTest, testInvocationAttr) {
    {
        string invStr = R"json({"op" : "printTableTvf", "params" : [], "type":"TVF"})json";
        InvocationAttr attr;
        autil::legacy::FastFromJsonString(attr, invStr);
        ASSERT_EQ("printTableTvf", attr.tvfName);
        ASSERT_EQ("TVF", attr.type);
        vector<string> params;
        vector<string> tables;
        ASSERT_TRUE(attr.parseStrParams(params, tables));
        vector<string> expect = {};
        vector<string> expectTable = {};
        ASSERT_EQ(expect, params);
        ASSERT_EQ(expectTable, tables);
    }
    {
        string invStr
            = R"json({"op" : "printTableTvf", "params" : ["@table#0"], "type":"TVF"})json";
        InvocationAttr attr;
        autil::legacy::FastFromJsonString(attr, invStr);
        ASSERT_EQ("printTableTvf", attr.tvfName);
        ASSERT_EQ("TVF", attr.type);
        vector<string> params;
        vector<string> tables;
        ASSERT_TRUE(attr.parseStrParams(params, tables));
        vector<string> expect = {};
        vector<string> expectTable = {"0"};
        ASSERT_EQ(expect, params);
        ASSERT_EQ(expectTable, tables);
    }
    {
        string invStr
            = R"json({"op" : "printTableTvf", "params" : ["1", "@table#0"], "type":"TVF"})json";
        InvocationAttr attr;
        autil::legacy::FastFromJsonString(attr, invStr);
        ASSERT_EQ("printTableTvf", attr.tvfName);
        ASSERT_EQ("TVF", attr.type);
        vector<string> params;
        vector<string> tables;
        ASSERT_TRUE(attr.parseStrParams(params, tables));
        vector<string> expect = {"1"};
        vector<string> expectTable = {"0"};
        ASSERT_EQ(expect, params);
        ASSERT_EQ(expectTable, tables);
    }
    {
        string invStr
            = R"json({"op" : "printTableTvf", "params" : ["1", "table#0"], "type":"TVF"})json";
        InvocationAttr attr;
        autil::legacy::FastFromJsonString(attr, invStr);
        ASSERT_EQ("printTableTvf", attr.tvfName);
        ASSERT_EQ("TVF", attr.type);
        vector<string> params;
        vector<string> tables;
        ASSERT_TRUE(attr.parseStrParams(params, tables));
        vector<string> expect = {"1", "table#0"};
        vector<string> expectTable = {};
        ASSERT_EQ(expect, params);
        ASSERT_EQ(expectTable, tables);
    }
    {
        string invStr
            = R"json({"op" : "printTableTvf", "params" : [1, "@table#0"], "type":"TVF"})json";
        InvocationAttr attr;
        autil::legacy::FastFromJsonString(attr, invStr);
        ASSERT_EQ("printTableTvf", attr.tvfName);
        ASSERT_EQ("TVF", attr.type);
        vector<string> params;
        vector<string> tables;
        ASSERT_FALSE(attr.parseStrParams(params, tables));
    }
}

} // namespace sql
