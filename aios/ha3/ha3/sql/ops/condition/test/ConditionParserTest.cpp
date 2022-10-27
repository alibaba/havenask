#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/condition/ConditionParser.h>
#include <ha3/sql/ops/condition/Condition.h>
#include <autil/legacy/RapidJsonHelper.h>

using namespace std;
using namespace suez::turing;
using namespace autil_rapidjson;

BEGIN_HA3_NAMESPACE(sql);

class ConditionParserTest : public TESTBASE {
public:
    ConditionParserTest();
    ~ConditionParserTest();
public:
    void setUp();
    void tearDown();
public:
    void checkLeafCondition(ConditionPtr condition, std::string str) {
        LeafCondition* leaf = dynamic_cast<LeafCondition*>(condition.get());
        const SimpleValue &simpleVal = leaf->getCondition();
        string str1 = RapidJsonHelper::SimpleValue2Str(simpleVal);
        ASSERT_EQ(str1, str1);
    }
};

ConditionParserTest::ConditionParserTest() {
}

ConditionParserTest::~ConditionParserTest() {
}

void ConditionParserTest::setUp() {
}

void ConditionParserTest::tearDown() {
}

TEST_F(ConditionParserTest, testSimpleProcess) {
    ConditionParser parser;
    ConditionPtr condition;
    string jsonStr = "{\"op\":\"=\",\"params\":[\"a.x\",\"b.y\"]}";
    bool success = parser.parseCondition(jsonStr, condition);
    ASSERT_TRUE(success);
    ASSERT_EQ(LEAF_CONDITION, condition->getType());
    string str = RapidJsonHelper::SimpleValue2Str(*(condition->_topDoc));
    checkLeafCondition(condition, str);
    ASSERT_EQ(jsonStr, condition->toString());
}

TEST_F(ConditionParserTest, testAndOrCondition) {
    ConditionParser parser;
    ConditionPtr condition;
    string jsonStr = "{\"op\" : \"AND\" , \"params\" : [\"a.x\", {\"op\" : \"OR\" , \"params\" : [\"b.y\" , \"c.z\"]}]}";
    bool success = parser.parseCondition(jsonStr, condition);
    ASSERT_TRUE(success);
    ASSERT_EQ(AND_CONDITION, condition->getType());
    vector<ConditionPtr> children = condition->getChildCondition();
    ASSERT_EQ(2, children.size());
    ASSERT_EQ(LEAF_CONDITION, children[0]->getType());
    checkLeafCondition(children[0], "a.x");

    ConditionPtr rightCond = children[1];
    children = rightCond->getChildCondition();
    ASSERT_EQ(OR_CONDITION, rightCond->getType());
    ASSERT_EQ(2, children.size());
    ASSERT_EQ(LEAF_CONDITION, children[0]->getType());
    ASSERT_EQ(LEAF_CONDITION, children[1]->getType());
    checkLeafCondition(children[0], "b.y");
    checkLeafCondition(children[1], "c.z");
    ASSERT_EQ( "(\"a.x\" AND (\"b.y\" OR \"c.z\"))", condition->toString());
}


TEST_F(ConditionParserTest, testNotCondition) {
    ConditionParser parser;
    ConditionPtr condition;
    string jsonStr = "{\"op\" : \"NOT\" , \"params\" : [\"b.y\"]}";
    bool success = parser.parseCondition(jsonStr, condition);
    ASSERT_TRUE(success);
    ASSERT_EQ(NOT_CONDITION, condition->getType());
    vector<ConditionPtr> children = condition->getChildCondition();
    ASSERT_EQ(1, children.size());
    ASSERT_EQ(LEAF_CONDITION, children[0]->getType());
    checkLeafCondition(children[0], "b.y");
    ASSERT_EQ( "NOT \"b.y\"", condition->toString());
}

TEST_F(ConditionParserTest, testNotAndOrCondition) {
    ConditionParser parser;
    ConditionPtr condition;
    string jsonStr = "{\"op\" : \"AND\" , \"params\" : [\"a.x\", {\"op\" : \"OR\" , \"params\" : [\"b.y\" , \"c.z\"]}]}";
    jsonStr = "{\"op\" : \"NOT\" , \"params\" : [" +jsonStr +"]}";
    bool success = parser.parseCondition(jsonStr, condition);
    ASSERT_TRUE(success);

    ASSERT_EQ(NOT_CONDITION, condition->getType());
    vector<ConditionPtr> children = condition->getChildCondition();
    ASSERT_EQ(1, children.size());
    ASSERT_EQ(AND_CONDITION, children[0]->getType());

    children = children[0]->getChildCondition();
    ASSERT_EQ(2, children.size());
    ASSERT_EQ(LEAF_CONDITION, children[0]->getType());
    checkLeafCondition(children[0], "a.x");

    ConditionPtr rightCond = children[1];
    children = rightCond->getChildCondition();
    ASSERT_EQ(OR_CONDITION, rightCond->getType());
    ASSERT_EQ(2, children.size());
    ASSERT_EQ(LEAF_CONDITION, children[0]->getType());
    ASSERT_EQ(LEAF_CONDITION, children[1]->getType());
    checkLeafCondition(children[0], "b.y");
    checkLeafCondition(children[1], "c.z");
    ASSERT_EQ( "NOT (\"a.x\" AND (\"b.y\" OR \"c.z\"))", condition->toString());
}

END_HA3_NAMESPACE(sql);
