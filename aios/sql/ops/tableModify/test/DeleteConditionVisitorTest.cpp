#include "sql/ops/tableModify/DeleteConditionVisitor.h"

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

class DeleteConditionVisitorTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
};

void DeleteConditionVisitorTest::setUp() {}

void DeleteConditionVisitorTest::tearDown() {}

TEST_F(DeleteConditionVisitorTest, testCompound) {
    navi::NaviLoggerProvider provider("ERROR");
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
    std::set<std::string> fetchField = {"a", "b"};
    DeleteConditionVisitor visitor(fetchField);
    cond->accept(&visitor);
    std::vector<std::map<std::string, std::string>> values;
    ASSERT_TRUE(visitor.fetchValues(values)) << StringUtil::toString(provider.getTrace(""));
    ASSERT_EQ("a:1;b:4;c:7; a:2;b:5;c:8; a:3;b:6;c:9;", StringUtil::toString(values));
}

TEST_F(DeleteConditionVisitorTest, testFetchValuesFailed) {
    navi::NaviLoggerProvider provider("ERROR");
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
    std::set<std::string> fetchField = {"a", "b", "c", "d"};
    DeleteConditionVisitor visitor(fetchField);
    cond->accept(&visitor);
    std::vector<std::map<std::string, std::string>> values;
    ASSERT_FALSE(visitor.fetchValues(values));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1,
                      "DELETE: fetch values [a:1;b:4;c:7; a:2;b:5;c:8; a:3;b:6;c:9;] not include "
                      "expected [a b c d] set",
                      traces);
}

} // namespace sql
