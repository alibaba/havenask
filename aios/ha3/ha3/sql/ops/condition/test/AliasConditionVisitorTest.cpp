#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/ops/condition/AliasConditionVisitor.h>
#include <ha3/sql/ops/condition/ConditionParser.h>

using namespace suez::turing;
using namespace std;
using namespace testing;
using namespace suez::turing;


BEGIN_HA3_NAMESPACE(sql);

class AliasConditionVisitorTest : public TESTBASE {
};


TEST_F(AliasConditionVisitorTest, testIsUdf) {
    autil_rapidjson::SimpleDocument simpleDoc;
    string indexCondition = R"json({"op":"=", "params":["$sum(id)",1]})json"; // query
    string attr1Condition = R"json({"op":">=", "params":["avg(attr1)",2]})json"; // filter
    string attr2Condition = R"json({"op":">=", "params":["$avg(attr2)",2]})json"; // filter
    string orCondition = "{\"op\":\"OR\", \"params\":[" + indexCondition + " , " + attr1Condition + "]}";
    string andCondition = "{\"op\":\"AND\", \"params\":[" + orCondition + " , " + attr2Condition + "]}";
    string notCondition = "{\"op\":\"NOT\", \"params\":[" + andCondition + "]}";
    autil::mem_pool::Pool pool;
    ConditionParser parser(&pool);
    ConditionPtr condition;
    ASSERT_TRUE(parser.parseCondition(notCondition, condition));

    AliasConditionVisitor vistor;
    condition->accept(&vistor);
    ASSERT_FALSE(vistor.isError());
    auto aliasMap = vistor.getAliasMap();
    ASSERT_EQ(2, aliasMap.size());
    ASSERT_EQ("sum(id)", aliasMap["sum_id_"]);
    ASSERT_EQ("avg(attr2)", aliasMap["avg_attr2_"]);
}



END_HA3_NAMESPACE();
