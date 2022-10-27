 #include <unittest/unittest.h>
#include <autil/StringUtil.h>
#include <ha3/test/test.h>
#include <ha3/sql/ops/condition/OutputFieldsVisitor.h>

using namespace std;
using namespace testing;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil_rapidjson;

BEGIN_HA3_NAMESPACE(sql);

class OutputFieldsVisitorTest : public TESTBASE {
public:
    OutputFieldsVisitorTest()
        : _allocator(&_pool)
    {}
public:
    void setUp() {}
    void tearDown() {}
public:
    SimpleDocument parseToSimpleDocument(const string &jsonStr) {
        SimpleDocument doc(&_allocator);
        doc.Parse(jsonStr.c_str());
        EXPECT_FALSE(doc.HasParseError());
        return doc;
    }
private:
    autil::mem_pool::Pool _pool;
    AutilPoolAllocator _allocator;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, OutputFieldsVisitorTest);

TEST_F(OutputFieldsVisitorTest, visitItemOp_Success) {
    string outputExprsJson = R"json({
        "op": "ITEM",
        "params": [
            "$tags",
            "host"
        ],
        "type": "OTHER"
    })json";
    const SimpleDocument doc = parseToSimpleDocument(outputExprsJson);
    OutputFieldsVisitor visitor;
    visitor.visitItemOp(doc);
    visitor.visitItemOp(doc); // dup
    ASSERT_FALSE(visitor.isError());
    auto &usedFieldsItemSet = visitor._usedFieldsItemSet;
    ASSERT_EQ(1, usedFieldsItemSet.size());
    ASSERT_NE(usedFieldsItemSet.end(), usedFieldsItemSet.find(make_pair("$tags", "host")));
}

TEST_F(OutputFieldsVisitorTest, visitItemOp_Error_IsNotItem) {
    string outputExprsJson = R"json({
        "op": "ITEM",
        "params": [
            "$tags",
            "host",
            "invalid"
        ],
        "type": "OTHER"
    })json";
    const SimpleDocument doc = parseToSimpleDocument(outputExprsJson);
    OutputFieldsVisitor visitor;
    visitor.visitItemOp(doc);
    ASSERT_TRUE(visitor.isError());
    ASSERT_NE(string::npos, visitor.errorInfo().find("parse item variable failed:"));
    auto &usedFieldsItemSet = visitor._usedFieldsItemSet;
    ASSERT_EQ(0, usedFieldsItemSet.size());
}

TEST_F(OutputFieldsVisitorTest, visitColumn_Success) {
    string outputExprsJson = R"json([
        "$value1",
        "$value2",
        "$value1",
        "$value2",
        "$value2"
    ])json";
    const SimpleDocument doc = parseToSimpleDocument(outputExprsJson);
    OutputFieldsVisitor visitor;
    for (size_t i = 0; i < doc.Size(); ++i) {
        visitor.visitColumn(doc[i]);
        ASSERT_FALSE(visitor.isError());
    }
    auto &usedFieldsColumnSet = visitor._usedFieldsColumnSet;
    ASSERT_EQ(2, usedFieldsColumnSet.size());
    ASSERT_NE(usedFieldsColumnSet.end(), usedFieldsColumnSet.find("$value1"));
    ASSERT_NE(usedFieldsColumnSet.end(), usedFieldsColumnSet.find("$value2"));
}

TEST_F(OutputFieldsVisitorTest, visitParams_Success) {
    string outputExprsJson = R"json({
    "op": "+",
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
            "op": "+",
            "params": [
                "$value",
                1
            ],
            "type": "OTHER"
        }
    ],
    "type": "OTHER"
})json";
    const SimpleDocument doc = parseToSimpleDocument(outputExprsJson);
    OutputFieldsVisitor visitor;
    visitor.visitParams(doc);
    ASSERT_FALSE(visitor.isError());
    auto &usedFieldsItemSet = visitor._usedFieldsItemSet;
    auto &usedFieldsColumnSet = visitor._usedFieldsColumnSet;
    ASSERT_EQ(1, usedFieldsItemSet.size());
    ASSERT_NE(usedFieldsItemSet.end(), usedFieldsItemSet.find(make_pair("$tags", "host")));
    ASSERT_EQ(1, usedFieldsColumnSet.size());
    ASSERT_NE(usedFieldsColumnSet.end(), usedFieldsColumnSet.find("$value"));
}

TEST_F(OutputFieldsVisitorTest, visitParams_Error_InvalidParam) {
    string outputExprsJson = R"json({
    "op": "+",
    "params": [
        {
            "op": "ITEM",
            "params": [
                "$tags"
            ],
            "type": "OTHER"
        },
        {
            "op": "+",
            "params": [
                "$value",
                1
            ],
            "type": "OTHER"
        }
    ],
    "type": "OTHER"
})json";
    const SimpleDocument doc = parseToSimpleDocument(outputExprsJson);
    OutputFieldsVisitor visitor;
    visitor.visitParams(doc);
    ASSERT_TRUE(visitor.isError());
    ASSERT_NE(string::npos, visitor.errorInfo().find("invalid ITEM op format:"));
    auto &usedFieldsItemSet = visitor._usedFieldsItemSet;
    auto &usedFieldsColumnSet = visitor._usedFieldsColumnSet;
    ASSERT_EQ(0, usedFieldsItemSet.size());
    ASSERT_EQ(0, usedFieldsColumnSet.size());
}

TEST_F(OutputFieldsVisitorTest, visit_Success) {
    string outputExprsJson = R"json({
    "op": "+",
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
            "op": "+",
            "params": [
                "$value",
                1
            ],
            "type": "OTHER"
        }
    ],
    "type": "OTHER"
})json";
    const SimpleDocument doc = parseToSimpleDocument(outputExprsJson);
    OutputFieldsVisitor visitor;
    visitor.visitParams(doc);
    ASSERT_FALSE(visitor.isError());
    auto &usedFieldsItemSet = visitor._usedFieldsItemSet;
    auto &usedFieldsColumnSet = visitor._usedFieldsColumnSet;
    ASSERT_EQ(1, usedFieldsItemSet.size());
    ASSERT_NE(usedFieldsItemSet.end(), usedFieldsItemSet.find(make_pair("$tags", "host")));
    ASSERT_EQ(1, usedFieldsColumnSet.size());
    ASSERT_NE(usedFieldsColumnSet.end(), usedFieldsColumnSet.find("$value"));
}

END_HA3_NAMESPACE();
