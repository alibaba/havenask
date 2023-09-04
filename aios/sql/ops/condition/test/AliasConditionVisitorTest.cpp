#include "sql/ops/condition/AliasConditionVisitor.h"

#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>

#include "autil/StringUtil.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "navi/util/NaviTestPool.h"
#include "sql/ops/condition/Condition.h"
#include "sql/ops/condition/ConditionParser.h"
#include "sql/ops/condition/ExprUtil.h"
#include "suez/turing/expression/common.h"
#include "unittest/unittest.h"

using namespace suez::turing;
using namespace std;
using namespace testing;
using namespace suez::turing;

namespace sql {

class AliasConditionVisitorTest : public TESTBASE {};

TEST_F(AliasConditionVisitorTest, testIsUdf) {
    autil::SimpleDocument simpleDoc;
    string indexCondition = R"json({"op":"=", "params":["$sum(id)",1]})json";     // query
    string attr1Condition = R"json({"op":">=", "params":["avg(attr1)",2]})json";  // filter
    string attr2Condition = R"json({"op":">=", "params":["$avg(attr2)",2]})json"; // filter
    string orCondition
        = "{\"op\":\"OR\", \"params\":[" + indexCondition + " , " + attr1Condition + "]}";
    string andCondition
        = "{\"op\":\"AND\", \"params\":[" + orCondition + " , " + attr2Condition + "]}";
    string notCondition = "{\"op\":\"NOT\", \"params\":[" + andCondition + "]}";
    autil::mem_pool::PoolAsan pool;
    ConditionParser parser(&pool);
    ConditionPtr condition;
    ASSERT_TRUE(parser.parseCondition(notCondition, condition));

    AliasConditionVisitor vistor;
    condition->accept(&vistor);
    ASSERT_FALSE(vistor.isError());
    auto aliasMap = vistor.getAliasMap();
    ASSERT_EQ(2, aliasMap.size());
    std::cout << autil::StringUtil::toString(aliasMap) << std::endl;
    std::string token1 = "sum(id)";
    std::string token2 = "avg(attr2)";
    ExprUtil::convertColumnName(token1);
    ExprUtil::convertColumnName(token2);
    ASSERT_EQ("sum(id)", aliasMap[token1]);
    ASSERT_EQ("avg(attr2)", aliasMap[token2]);
}

} // namespace sql
