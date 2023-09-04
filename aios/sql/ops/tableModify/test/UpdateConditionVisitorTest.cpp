#include "sql/ops/tableModify/UpdateConditionVisitor.h"

#include <cstddef>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "navi/log/NaviLoggerProvider.h"
#include "sql/ops/condition/Condition.h"
#include "sql/ops/condition/ConditionParser.h"
#include "sql/ops/test/OpTestBase.h"
#include "unittest/unittest.h"

using namespace autil;
using namespace std;

namespace sql {

class UpdateConditionVisitorTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
};

void UpdateConditionVisitorTest::setUp() {}

void UpdateConditionVisitorTest::tearDown() {}

TEST_F(UpdateConditionVisitorTest, testHasSameFields) {
    ASSERT_TRUE(UpdateConditionVisitor::hasSameFields({{"a", "1"}}, {{"b", "1"}, {"a", "1"}}));
    ASSERT_FALSE(UpdateConditionVisitor::hasSameFields({{"a", "1"}}, {{"b", "1"}}));
    ASSERT_FALSE(UpdateConditionVisitor::hasSameFields({}, {{"b", "1"}}));
    ASSERT_FALSE(UpdateConditionVisitor::hasSameFields({{"b", "1"}}, {}));
    ASSERT_TRUE(
        UpdateConditionVisitor::hasSameFields({{"c", "2"}, {"a", "1"}}, {{"b", "1"}, {"a", "1"}}));
    ASSERT_FALSE(
        UpdateConditionVisitor::hasSameFields({{"c", "2"}, {"d", "1"}}, {{"b", "1"}, {"a", "1"}}));
}

TEST_F(UpdateConditionVisitorTest, testIsIncludeFields) {
    ASSERT_TRUE(UpdateConditionVisitor::isIncludeFields({{"b", "1"}, {"a", "2"}}, {{"a", "1"}}));
    ASSERT_FALSE(UpdateConditionVisitor::isIncludeFields({{"a", "1"}}, {{"b", "1"}, {"a", "2"}}));
    ASSERT_FALSE(UpdateConditionVisitor::isIncludeFields({{"b", "1"}, {"a", "2"}},
                                                         {{"a", "1"}, {"c", "2"}}));
    ASSERT_TRUE(UpdateConditionVisitor::isIncludeFields({{"b", "1"}, {"a", "2"}},
                                                        {{"a", "1"}, {"b", "2"}}));
    ASSERT_TRUE(UpdateConditionVisitor::isIncludeFields({{"b", "1"}, {"a", "2"}}, {}));
    ASSERT_FALSE(UpdateConditionVisitor::isIncludeFields({}, {{"b", "1"}, {"a", "2"}}));
}

TEST_F(UpdateConditionVisitorTest, testIsSameFields) {
    ASSERT_TRUE(
        UpdateConditionVisitor::isSameFields({{"b", "1"}, {"a", "2"}}, {{"a", "1"}, {"b", "2"}}));
    ASSERT_FALSE(
        UpdateConditionVisitor::isSameFields({{"b", "1"}, {"a", "2"}}, {{"a", "1"}, {"c", "2"}}));
    ASSERT_FALSE(UpdateConditionVisitor::isSameFields({{"b", "1"}, {"a", "2"}}, {{"a", "1"}}));
    ASSERT_FALSE(UpdateConditionVisitor::isSameFields({{"a", "1"}}, {{"b", "1"}, {"a", "2"}}));
    ASSERT_FALSE(UpdateConditionVisitor::isSameFields({}, {{"b", "1"}, {"a", "2"}}));
    ASSERT_FALSE(UpdateConditionVisitor::isSameFields({{"b", "1"}, {"a", "2"}}, {}));
    ASSERT_TRUE(UpdateConditionVisitor::isSameFields({}, {}));
}

TEST_F(UpdateConditionVisitorTest, testLeafCond_equal) {
    ConditionParser parser;
    string jsonStr = R"(
{
    "op":"=",
    "params":[
        "$a",
        1
    ]
}
)";
    ConditionPtr cond;
    bool success = parser.parseCondition(jsonStr, cond);
    ASSERT_TRUE(success);
    std::set<std::string> fetchField = {"a"};
    UpdateConditionVisitor visitor(fetchField);
    cond->accept(&visitor);
    std::vector<std::map<std::string, std::string>> values;
    ASSERT_TRUE(visitor.fetchValues(values));
    ASSERT_EQ("a:1;", StringUtil::toString(values));
}

TEST_F(UpdateConditionVisitorTest, testLeafCond_equal_string) {
    ConditionParser parser;
    string jsonStr = R"(
{
    "op":"=",
    "params":[
        "$a",
        "abc"
    ]
}
)";
    ConditionPtr cond;
    bool success = parser.parseCondition(jsonStr, cond);
    ASSERT_TRUE(success);
    std::set<std::string> fetchField = {"a"};
    UpdateConditionVisitor visitor(fetchField);
    cond->accept(&visitor);
    std::vector<std::map<std::string, std::string>> values;
    ASSERT_TRUE(visitor.fetchValues(values));
    ASSERT_EQ("a:abc;", StringUtil::toString(values));
}

TEST_F(UpdateConditionVisitorTest, testLeafCond_right_attr) {
    ConditionParser parser;
    string jsonStr = R"(
{
    "op":"=",
    "params":[
        1,
        "$a"
    ]
}
)";
    ConditionPtr cond;
    bool success = parser.parseCondition(jsonStr, cond);
    ASSERT_TRUE(success);
    std::set<std::string> fetchField = {"a"};
    UpdateConditionVisitor visitor(fetchField);
    cond->accept(&visitor);
    std::vector<std::map<std::string, std::string>> values;
    ASSERT_TRUE(visitor.fetchValues(values));
    ASSERT_EQ("a:1;", StringUtil::toString(values));
}

TEST_F(UpdateConditionVisitorTest, testLeafCond_CAST) {
    navi::NaviLoggerProvider provider("ERROR");
    ConditionParser parser;
    string jsonStr = R"(
{
    "op":"=",
    "params":[
        "$a",
        {
            "op":"CAST",
            "type":"UDF",
            "params": [1],
            "cast_type":"BIGINT"
        }
    ]
}
)";
    ConditionPtr cond;
    bool success = parser.parseCondition(jsonStr, cond);
    ASSERT_TRUE(success);
    std::set<std::string> fetchField = {"a"};
    UpdateConditionVisitor visitor(fetchField);
    cond->accept(&visitor);
    std::vector<std::map<std::string, std::string>> values;
    ASSERT_TRUE(visitor.fetchValues(values)) << StringUtil::toString(provider.getTrace(""));
    ASSERT_EQ("a:1;", StringUtil::toString(values));
}

TEST_F(UpdateConditionVisitorTest, testLeafCond_equal_no_field) {
    ConditionParser parser;
    string jsonStr = R"(
{
    "op":"=",
    "params":[
        "a",
        1
    ]
}
)";
    ConditionPtr cond;
    bool success = parser.parseCondition(jsonStr, cond);
    ASSERT_TRUE(success);
    std::set<std::string> fetchField = {"a"};
    UpdateConditionVisitor visitor(fetchField);
    cond->accept(&visitor);
    ASSERT_TRUE(visitor.isError());
}

TEST_F(UpdateConditionVisitorTest, testLeafCond_in) {
    ConditionParser parser;
    string jsonStr = R"(
{
    "op":"IN",
    "params":[
        "$a",
        1,
        2,
        3
    ]
}
)";
    ConditionPtr cond;
    bool success = parser.parseCondition(jsonStr, cond);
    ASSERT_TRUE(success);
    std::set<std::string> fetchField = {"a"};
    UpdateConditionVisitor visitor(fetchField);
    cond->accept(&visitor);
    std::vector<std::map<std::string, std::string>> values;
    ASSERT_TRUE(visitor.fetchValues(values));
    ASSERT_EQ("a:1; a:2; a:3;", StringUtil::toString(values));
}

TEST_F(UpdateConditionVisitorTest, testLeafCond_in_single) {
    ConditionParser parser;
    string jsonStr = R"(
{
    "op":"IN",
    "params":[
        "$a",
        1
    ]
}
)";
    ConditionPtr cond;
    bool success = parser.parseCondition(jsonStr, cond);
    ASSERT_TRUE(success);
    std::set<std::string> fetchField = {"a"};
    UpdateConditionVisitor visitor(fetchField);
    cond->accept(&visitor);
    std::vector<std::map<std::string, std::string>> values;
    ASSERT_TRUE(visitor.fetchValues(values));
    ASSERT_EQ("a:1;", StringUtil::toString(values));
}

TEST_F(UpdateConditionVisitorTest, testNotCond) {
    ConditionParser parser;
    string jsonStr = R"(
{
    "op":"NOT",
    "params":[
        "$a"
    ]
}
)";
    ConditionPtr cond;
    bool success = parser.parseCondition(jsonStr, cond);
    ASSERT_TRUE(success);
    std::set<std::string> fetchField = {"a"};
    UpdateConditionVisitor visitor(fetchField);
    cond->accept(&visitor);
    ASSERT_TRUE(visitor.isError());
}

TEST_F(UpdateConditionVisitorTest, testOrCond_childrenError) {
    ConditionParser parser;
    string jsonStr = R"(
{
    "op":"OR",
    "params":[
        {
            "op":"=",
            "params":[
                "$a",
                1
            ]
        },
        {
            "op":"=",
            "params":[
                "$b",
                2
            ]
        },
        {
            "op":"=",
            "params":[
                "$a",
                3
            ]
        }
    ]
}
)";
    ConditionPtr cond;
    bool success = parser.parseCondition(jsonStr, cond);
    ASSERT_TRUE(success);
    std::set<std::string> fetchField = {"a"};
    UpdateConditionVisitor visitor(fetchField);
    cond->accept(&visitor);
    ASSERT_TRUE(visitor.isError());
}

TEST_F(UpdateConditionVisitorTest, testOrCond_notSameField) {
    navi::NaviLoggerProvider provider("ERROR");
    ConditionParser parser;
    string jsonStr = R"(
{
    "op":"OR",
    "params":[
        {
            "op":"=",
            "params":[
                "$a",
                1
            ]
        },
        {
            "op":"=",
            "params":[
                "$b",
                2
            ]
        },
        {
            "op":"=",
            "params":[
                "$a",
                3
            ]
        }
    ]
}
)";
    ConditionPtr cond;
    bool success = parser.parseCondition(jsonStr, cond);
    ASSERT_TRUE(success);
    std::set<std::string> fetchField = {"a", "b"};
    UpdateConditionVisitor visitor(fetchField);
    cond->accept(&visitor);
    ASSERT_TRUE(visitor.isError());
    ASSERT_EQ("Update failed: [b:2;] can not insert to [a:1;]", visitor.errorInfo());
}

TEST_F(UpdateConditionVisitorTest, testOrCond) {
    navi::NaviLoggerProvider provider("ERROR");
    ConditionParser parser;
    string jsonStr = R"(
{
    "op":"OR",
    "params":[
        {
            "op":"=",
            "params":[
                "$a",
                1
            ]
        },
        {
            "op":"IN",
            "params":[
                "$a",
                2,
                4
            ]
        },
        {
            "op":"=",
            "params":[
                "$a",
                3
            ]
        }
    ]
}
)";
    ConditionPtr cond;
    bool success = parser.parseCondition(jsonStr, cond);
    ASSERT_TRUE(success);
    std::set<std::string> fetchField = {"a"};
    UpdateConditionVisitor visitor(fetchField);
    cond->accept(&visitor);
    std::vector<std::map<std::string, std::string>> values;
    ASSERT_TRUE(visitor.fetchValues(values));
    ASSERT_EQ("a:1; a:2; a:4; a:3;", StringUtil::toString(values));
}

TEST_F(UpdateConditionVisitorTest, testAndCond_ERROR_childrenError) {
    navi::NaviLoggerProvider provider("ERROR");
    ConditionParser parser;
    string jsonStr = R"(
{
    "op":"AND",
    "params":[
        {
            "op":"IN",
            "params":[
                "b",
                2,
                4
            ]
        }
    ]
}
)";
    ConditionPtr cond;
    bool success = parser.parseCondition(jsonStr, cond);
    ASSERT_TRUE(success);
    std::set<std::string> fetchField = {"a"};
    UpdateConditionVisitor visitor(fetchField);
    cond->accept(&visitor);
    ASSERT_TRUE(visitor.isError());
    ASSERT_EQ("Update not supported: {\"op\":\"IN\",\"params\":[\"b\",2,4]}", visitor.errorInfo());
}

TEST_F(UpdateConditionVisitorTest, testAndCond_ERROR_multiValue) {
    navi::NaviLoggerProvider provider("ERROR");
    ConditionParser parser;
    string jsonStr = R"(
{
    "op":"AND",
    "params":[
        {
            "op":"IN",
            "params":[
                "$a",
                2,
                4
            ]
        }
    ]
}
)";
    ConditionPtr cond;
    bool success = parser.parseCondition(jsonStr, cond);
    ASSERT_TRUE(success);
    std::set<std::string> fetchField = {"a"};
    UpdateConditionVisitor visitor(fetchField);
    cond->accept(&visitor);
    ASSERT_TRUE(visitor.isError());
    ASSERT_EQ("Update failed: `and` only support single key", visitor.errorInfo());
}

TEST_F(UpdateConditionVisitorTest, testAndCond_ERROR_SameFields) {
    navi::NaviLoggerProvider provider("ERROR");
    ConditionParser parser;
    string jsonStr = R"(
{
    "op":"AND",
    "params":[
        {
            "op":"=",
            "params":[
                "$a",
                1
            ]
        },
        {
            "op":"IN",
            "params":[
                "$a",
                2
            ]
        }
    ]
}
)";
    ConditionPtr cond;
    bool success = parser.parseCondition(jsonStr, cond);
    ASSERT_TRUE(success);
    std::set<std::string> fetchField = {"a"};
    UpdateConditionVisitor visitor(fetchField);
    cond->accept(&visitor);
    ASSERT_TRUE(visitor.isError());
    ASSERT_EQ("Update failed: `and` not support same key, [a:2;] [a:1;]", visitor.errorInfo());
}

TEST_F(UpdateConditionVisitorTest, testAndCond) {
    navi::NaviLoggerProvider provider("ERROR");
    ConditionParser parser;
    string jsonStr = R"(
{
    "op":"AND",
    "params":[
        {
            "op":"=",
            "params":[
                "$a",
                1
            ]
        },
        {
            "op":"IN",
            "params":[
                "$b",
                2
            ]
        }
    ]
}
)";
    ConditionPtr cond;
    bool success = parser.parseCondition(jsonStr, cond);
    ASSERT_TRUE(success);
    std::set<std::string> fetchField = {"a", "b"};
    UpdateConditionVisitor visitor(fetchField);
    cond->accept(&visitor);
    std::vector<std::map<std::string, std::string>> values;
    ASSERT_TRUE(visitor.fetchValues(values));
    ASSERT_EQ("a:1;b:2;", StringUtil::toString(values));
}

TEST_F(UpdateConditionVisitorTest, testAndCondNest) {
    navi::NaviLoggerProvider provider("ERROR");
    ConditionParser parser;
    string jsonStr = R"(
{
    "op":"AND",
    "params":[
        {
            "op":"=",
            "params":[
                "$a",
                1
            ]
        },
        {
            "op":"AND",
            "params":[
                {
                    "op":"IN",
                    "params":[
                        "$b",
                        2
                    ]
                },
                {
                    "op":"=",
                    "params":[
                        "$c",
                        3
                    ]
                }
            ]
        }
    ]
}
)";
    ConditionPtr cond;
    bool success = parser.parseCondition(jsonStr, cond);
    ASSERT_TRUE(success);
    std::set<std::string> fetchField = {"a", "b", "c"};
    UpdateConditionVisitor visitor(fetchField);
    cond->accept(&visitor);
    std::vector<std::map<std::string, std::string>> values;
    ASSERT_TRUE(visitor.fetchValues(values));
    ASSERT_EQ("a:1;b:2;c:3;", StringUtil::toString(values));
}

TEST_F(UpdateConditionVisitorTest, testCompound) {
    ConditionParser parser;
    string jsonStr = R"(
{
    "op":"OR",
    "params":[
        {
            "op":"AND",
            "params":[
                {
                    "op":"=",
                    "params":[
                        "$a",
                        1
                    ]
                },
                {
                    "op":"=",
                    "params":[
                        "$b",
                        4
                    ]
                }
            ]
        },
        {
            "op":"AND",
            "params":[
                {
                    "op":"=",
                    "params":[
                        "$a",
                        2
                    ]
                },
                {
                    "op":"=",
                    "params":[
                        "$b",
                        5
                    ]
                }
            ]
        },
        {
            "op":"AND",
            "params":[
                {
                    "op":"=",
                    "params":[
                        "$a",
                        3
                    ]
                },
                {
                    "op":"=",
                    "params":[
                        "$b",
                        6
                    ]
                }
            ]
        }
    ]
}
)";
    ConditionPtr cond;
    bool success = parser.parseCondition(jsonStr, cond);
    ASSERT_TRUE(success);
    std::set<std::string> fetchField = {"a", "b"};
    UpdateConditionVisitor visitor(fetchField);
    cond->accept(&visitor);
    std::vector<std::map<std::string, std::string>> values;
    ASSERT_TRUE(visitor.fetchValues(values));
    ASSERT_EQ("a:1;b:4; a:2;b:5; a:3;b:6;", StringUtil::toString(values));
}

TEST_F(UpdateConditionVisitorTest, testCompound2) {
    ConditionParser parser;
    string jsonStr = R"(
{
    "op":"OR",
    "params":[
        {
            "op":"AND",
            "params":[
                {
                    "op":"=",
                    "params":[
                        "$a",
                        1
                    ]
                },
                {
                    "op":"=",
                    "params":[
                        "$c",
                        "7"
                    ]
                },
                {
                    "op":"=",
                    "params":[
                        "$b",
                        4
                    ]
                }
            ]
        },
        {
            "op":"AND",
            "params":[
                {
                    "op":"=",
                    "params":[
                        "$c",
                        "8"
                    ]
                },
                {
                    "op":"=",
                    "params":[
                        "$a",
                        2
                    ]
                },
                {
                    "op":"=",
                    "params":[
                        "$b",
                        5
                    ]
                }
            ]
        },
        {
            "op":"AND",
            "params":[
                {
                    "op":"=",
                    "params":[
                        "$a",
                        3
                    ]
                },
                {
                    "op":"=",
                    "params":[
                        "$b",
                        6
                    ]
                },
                {
                    "op":"=",
                    "params":[
                        "$c",
                        "9"
                    ]
                }
            ]
        }
    ]
}
)";
    ConditionPtr cond;
    bool success = parser.parseCondition(jsonStr, cond);
    ASSERT_TRUE(success);
    std::set<std::string> fetchField = {"a", "b", "c"};
    UpdateConditionVisitor visitor(fetchField);
    cond->accept(&visitor);
    std::vector<std::map<std::string, std::string>> values;
    ASSERT_TRUE(visitor.fetchValues(values));
    ASSERT_EQ("a:1;b:4;c:7; a:2;b:5;c:8; a:3;b:6;c:9;", StringUtil::toString(values));
}

} // namespace sql
