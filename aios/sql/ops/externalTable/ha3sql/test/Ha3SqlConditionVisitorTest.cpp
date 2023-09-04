#include "sql/ops/externalTable/ha3sql/Ha3SqlConditionVisitor.h"

#include <iosfwd>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "navi/common.h"
#include "navi/util/NaviTestPool.h"
#include "sql/ops/condition/Condition.h"
#include "sql/ops/condition/ConditionParser.h"
#include "sql/ops/condition/ExprUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace navi;
using namespace testing;

namespace sql {

class Ha3SqlConditionVisitorTest : public TESTBASE {
public:
    Ha3SqlConditionVisitorTest() {}
    ~Ha3SqlConditionVisitorTest() {}
};

TEST_F(Ha3SqlConditionVisitorTest, testVisit_Success_Simple) {
    // SELECT ...
    // WHERE tags['host'] = literal_or('10.10*')
    string conditionJson = R"json({
        "op":"=",
        "params":[
             {
                 "op":"ITEM",
                 "params":[
                     "$tags",
                     "host"
                 ],
                 "type":"OTHER"
             },
             {
                 "op":"literal_or",
                 "params":["10.10*"],
                 "type":"UDF"
             }
        ],
        "type":"OTHER"
    })json";
    autil::mem_pool::PoolAsan pool;
    ConditionParser parser(&pool);
    ConditionPtr condition;
    ASSERT_TRUE(parser.parseCondition(conditionJson, condition));
    ASSERT_NE(nullptr, condition);

    auto pool2 = std::make_shared<autil::mem_pool::PoolAsan>();
    Ha3SqlConditionVisitor visitor(std::move(pool2));
    condition->accept(&visitor);
    ASSERT_FALSE(visitor.isError()) << visitor.errorInfo();
    ASSERT_EQ(R"str((`tags`['host']=literal_or(?)))str", visitor.getConditionStr());
    ASSERT_EQ(R"json([["10.10*"]])json", visitor.getDynamicParamsStr());
}

TEST_F(Ha3SqlConditionVisitorTest, testVisit_Success_Simple2) {
    // SELECT ...
    // WHERE tags['ho''st'] = literal_or('10.10*')
    string conditionJson = R"json({
        "op":"=",
        "params":[
             {
                 "op":"ITEM",
                 "params":[
                     "$tags",
                     "ho'st"
                 ],
                 "type":"OTHER"
             },
             {
                 "op":"literal_or",
                 "params":["10.10*"],
                 "type":"UDF"
             }
        ],
        "type":"OTHER"
    })json";
    autil::mem_pool::PoolAsan pool;
    ConditionParser parser(&pool);
    ConditionPtr condition;
    ASSERT_TRUE(parser.parseCondition(conditionJson, condition));
    ASSERT_NE(nullptr, condition);

    auto pool2 = std::make_shared<autil::mem_pool::PoolAsan>();
    Ha3SqlConditionVisitor visitor(std::move(pool2));
    condition->accept(&visitor);
    ASSERT_FALSE(visitor.isError()) << visitor.errorInfo();
    ASSERT_EQ(R"str((`tags`['ho''st']=literal_or(?)))str", visitor.getConditionStr());
    ASSERT_EQ(R"json([["10.10*"]])json", visitor.getDynamicParamsStr());
}

TEST_F(Ha3SqlConditionVisitorTest, testVisit_IN_Simple) {
    // SELECT ...
    // WHERE nid in (1,2)
    string conditionJson = R"json({
        "op":"IN",
        "params":[
            "$nid",
            1,
            2
        ],
        "type":"OTHER"
    })json";
    autil::mem_pool::PoolAsan pool;
    ConditionParser parser(&pool);
    ConditionPtr condition;
    ASSERT_TRUE(parser.parseCondition(conditionJson, condition));
    ASSERT_NE(nullptr, condition);

    auto pool2 = std::make_shared<autil::mem_pool::PoolAsan>();
    Ha3SqlConditionVisitor visitor(std::move(pool2));
    condition->accept(&visitor);
    ASSERT_FALSE(visitor.isError()) << visitor.errorInfo();
    ASSERT_EQ(R"str(`nid` in (?,?))str", visitor.getConditionStr());
    ASSERT_EQ(R"json([[1,2]])json", visitor.getDynamicParamsStr());
}

TEST_F(Ha3SqlConditionVisitorTest, testVisit_Success_Complex) {
    // SELECT ...
    // WHERE tags['host'] = literal_or("10.10.10.1|10.10.10.2")
    // AND `back``tick` = 123456
    // AND tags['app'] = "bs"
    // AND timestamp_larger_equal_than(1546272000)
    // AND timestamp_less_than(1546272025)
    string conditionJson = R"json({
    "op": "AND",
    "params": [
        {
            "op": "=",
            "params": [
                {
                    "op": "ITEM",
                    "params": [
                        "$tags",
                        "host"
                    ],
                    "type": "OTHER"
                },
                {
                    "op":"literal_or",
                    "params":[
                        "10.10.10.1|10.10.10.2"
                    ],
                    "type":"UDF"
                }
            ],
            "type": "OTHER"
        },
        {
            "op": "=",
            "params": [
                "$back`tick",
                123456
            ],
            "type": "OTHER"
        },
        {
            "op": "=",
            "params": [
                {
                    "op": "ITEM",
                    "params": [
                        "$tags",
                        "app"
                    ],
                    "type": "OTHER"
                },
                "bs"
            ],
            "type": "OTHER"
        },
        {
            "op":
            "timestamp_larger_equal_than",
            "params":
            [
                1546272000
            ],
            "type":
            "UDF"
        },
        {
            "op":
            "timestamp_less_than",
            "params":
            [
                1546272025
            ],
            "type":
            "UDF"
        }
    ],
    "type": "OTHER"
})json";
    autil::mem_pool::PoolAsan pool;
    ConditionParser parser(&pool);
    ConditionPtr condition;
    ASSERT_TRUE(parser.parseCondition(conditionJson, condition));
    ASSERT_NE(nullptr, condition);

    auto pool2 = std::make_shared<autil::mem_pool::PoolAsan>();
    Ha3SqlConditionVisitor visitor(std::move(pool2));
    condition->accept(&visitor);
    ASSERT_FALSE(visitor.isError()) << visitor.errorInfo();
    ASSERT_EQ(
        R"str(((`tags`['host']=literal_or(?)) AND (`back``tick`=?) AND (`tags`['app']=?) AND timestamp_larger_equal_than(?) AND timestamp_less_than(?)))str",
        visitor.getConditionStr());
    ASSERT_EQ(R"json([["10.10.10.1|10.10.10.2",123456,"bs",1546272000,1546272025]])json",
              visitor.getDynamicParamsStr());
}

TEST_F(Ha3SqlConditionVisitorTest, testVisitLeafCondition_Error) {
    // SELECT ...
    // WHERE tags['host'] = literal_or('10.10*')
    string conditionJson = R"json({
        "op":"=",
        "params":[
             {
                 "op":"ITEM",
                 "params":[
                     "$tags",
                     "host"
                 ],
                 "type":"OTHER"
             },
             {
                 "op":"literal_or",
                 "params":["10.10*"],
                 "type":"UDF"
             }
        ],
        "type":"OTHER"
    })json";
    autil::mem_pool::PoolAsan pool;
    ConditionParser parser(&pool);
    ConditionPtr condition;
    ASSERT_TRUE(parser.parseCondition(conditionJson, condition));
    ASSERT_NE(nullptr, condition);

    auto pool2 = std::make_shared<autil::mem_pool::PoolAsan>();
    Ha3SqlConditionVisitor visitor(std::move(pool2));
    std::string newColumnName = "`tags`['host']";
    ExprUtil::convertColumnName(newColumnName);
    ASSERT_NE("`tags`['host']", newColumnName);
    ASSERT_TRUE(visitor._renameMap.emplace(newColumnName,
                                           "aaaa")
                    .second); // key: make rename map conflict
    condition->accept(&visitor);
    ASSERT_TRUE(visitor.isError());
    ASSERT_THAT(visitor.errorInfo(), HasSubstr("to expr string failed"));
}

} // namespace sql
