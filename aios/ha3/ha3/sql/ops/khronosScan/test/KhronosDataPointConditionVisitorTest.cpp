#include <unittest/unittest.h>
#include <autil/StringUtil.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/test/test.h>
#include <ha3/sql/ops/khronosScan/KhronosDataPointConditionVisitor.h>
#include <ha3/sql/ops/condition/ConditionParser.h>

using namespace std;
using namespace testing;
using namespace khronos::search;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil_rapidjson;

BEGIN_HA3_NAMESPACE(sql);

class KhronosDataPointConditionVisitorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
public:
    template <typename F>
    void visitLeafCondition(const std::string &conditionJson, F func)
    {
        ConditionPtr cond;
        ASSERT_TRUE(_parser.parseCondition(conditionJson, cond));
        auto *leafCond = dynamic_cast<LeafCondition *>(cond.get());
        ASSERT_NE(nullptr, leafCond);
        auto &condition = leafCond->getCondition();
        ASSERT_NO_FATAL_FAILURE(func(condition));
    }

    void tryVisitCondition(const std::string &conditionJson,
                           KhronosDataPointConditionVisitorPtr &visitor)
    {
        ConditionPtr cond;
        ASSERT_TRUE(_parser.parseCondition(conditionJson, cond));
        ASSERT_NE(nullptr, cond);
        visitor.reset(new KhronosDataPointConditionVisitor(&_pool, _indexInfos));
        cond->accept(visitor.get());
    }
    void tryParseAsTimestamp(const std::string &conditionJson,
                             KhronosDataPointConditionVisitorPtr &visitor)
    {
        visitor.reset(new KhronosDataPointConditionVisitor(&_pool, _indexInfos));
        ASSERT_NO_FATAL_FAILURE(
                visitLeafCondition(conditionJson, [&] (const SimpleValue &condition) {
                            string op = condition[SQL_CONDITION_OPERATOR].GetString();
                            const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
                            visitor->parseAsTimestamp(param[0], param[1], op, op);
                        }));
    }
    void tryVisitLeafCondition(const std::string &conditionJson,
                               KhronosDataPointConditionVisitorPtr &visitor)
    {
        ConditionPtr cond;
        ASSERT_TRUE(_parser.parseCondition(conditionJson, cond));
        auto *leafCond = dynamic_cast<LeafCondition *>(cond.get());
        ASSERT_NE(nullptr, leafCond);

        visitor.reset(new KhronosDataPointConditionVisitor(&_pool, _indexInfos));
        visitor->visitLeafCondition(leafCond);
    }

private:
    autil::mem_pool::Pool _pool;
    IndexInfoMapType _indexInfos;
    ConditionParser _parser;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(khronosScan, KhronosDataPointConditionVisitorTest);

void KhronosDataPointConditionVisitorTest::setUp() {
    string indexInfosStr = R"json({
    "$tagv" : {
        "name" : "tagv",
        "type" : "khronos_tag_value"
    },
    "$metric" : {
        "name" : "metric",
        "type" : "khronos_metric"
    },
    "$ts" : {
        "name" : "ts",
        "type" : "khronos_timestamp"
    },
    "$tagk" : {
        "name" : "tagk",
        "type" : "khronos_tag_key"
    }
})json";
    FromJsonString(_indexInfos, indexInfosStr);
}

void KhronosDataPointConditionVisitorTest::tearDown() {
}

TEST_F(KhronosDataPointConditionVisitorTest, testVisitConditionSuccess) {
    string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "<",
            "params" :
            [
                1546272100,
                "$ts"
            ]
        },
        {
            "op" : "<=",
            "params" :
            [
                "$ts",
                1546272200
            ]
        },
        {
            "op":"LIKE",
            "params":[
                "10.10*",
                 {
                     "op":"ITEM",
                     "params":[
                         "$tags",
                         "host"
                     ],
                     "type":"OTHER"
                 }
            ],
            "type":"OTHER"
        },
        {
            "op":"LIKE",
            "params":[
                "*",
                 {
                     "op":"ITEM",
                     "params":[
                         "$tags",
                         "host2"
                     ],
                     "type":"OTHER"
                 }
            ],
            "type":"OTHER"
        },
        {
            "op":"=",
            "params":[
                 {
                     "op":"ITEM",
                     "params":[
                         "$tags",
                         "app"
                     ],
                     "type":"OTHER"
                 },
                 {
                     "op":"literal_or",
                     "params":[
                         "aaa|bbb"
                     ],
                     "type":"UDF"
                 }
            ],
            "type":"OTHER"
        },
        {
            "op":"=",
            "params":[
                 {
                     "op":"ITEM",
                     "params":[
                         "$tags",
                         "app1"
                     ],
                     "type":"OTHER"
                 },
                 {
                     "op":"literal_or",
                     "params":[
                         "*"
                     ],
                     "type":"UDF"
                 }
            ],
            "type":"OTHER"
        }

    ]
})json";
    KhronosDataPointConditionVisitorPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryVisitCondition(conditionJson, visitor));
    ASSERT_FALSE(visitor->isError()) << visitor->errorInfo();
    ASSERT_EQ(1546272101, visitor->_tsRange.begin);
    ASSERT_EQ(1546272201, visitor->_tsRange.end);
    auto &tagKVInfos = visitor->_tagKVInfos;
    ASSERT_EQ(4, tagKVInfos.size());
    ASSERT_EQ(string("host"), tagKVInfos[0].first.c_str());
    ASSERT_EQ(string("10.10*"), tagKVInfos[0].second.mTagvPattern.c_str());
    ASSERT_EQ(SearchInterface::LITERAL, tagKVInfos[0].second.mTagvMatchType);
    ASSERT_EQ(string("host2"), tagKVInfos[1].first.c_str());
    ASSERT_EQ(string("*"), tagKVInfos[1].second.mTagvPattern.c_str());
    ASSERT_EQ(SearchInterface::LITERAL, tagKVInfos[1].second.mTagvMatchType);
    ASSERT_EQ(string("app"), tagKVInfos[2].first.c_str());
    ASSERT_EQ(string("aaa|bbb"), tagKVInfos[2].second.mTagvPattern.c_str());
    ASSERT_EQ(SearchInterface::LITERAL_OR, tagKVInfos[2].second.mTagvMatchType);
    ASSERT_EQ(string("app1"), tagKVInfos[3].first.c_str());
    ASSERT_EQ(string("*"), tagKVInfos[3].second.mTagvPattern.c_str());
    ASSERT_EQ(SearchInterface::LITERAL_OR, tagKVInfos[3].second.mTagvMatchType);
}

TEST_F(KhronosDataPointConditionVisitorTest, testVisitLeafConditionError_ParamNotArray) {
    string conditionJson = R"json({
    "op" : "<=",
    "params" : {}
})json";
    KhronosDataPointConditionVisitorPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryVisitLeafCondition(conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_NE(string::npos, visitor->errorInfo().find("is not array in op [<=]"));
}

TEST_F(KhronosDataPointConditionVisitorTest, testVisitLeafConditionError_ParamSizeNotTwo) {
    string conditionJson = R"json({
    "op" : "<=",
    "params" : []
})json";
    KhronosDataPointConditionVisitorPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryVisitLeafCondition(conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_NE(string::npos, visitor->errorInfo().find("size is not 2 in op [<=]"));
}

TEST_F(KhronosDataPointConditionVisitorTest, testVisitLeafConditionError_ReverseOpFailed) {
    string conditionJson = R"json({
    "op" : "+",
    "params" :
    [
        200,
        "$ts"
    ]
})json";
    KhronosDataPointConditionVisitorPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryVisitLeafCondition(conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_EQ("reverse op [+] failed", visitor->errorInfo());
}

TEST_F(KhronosDataPointConditionVisitorTest, testVisitLeafConditionError_TypeNotRecognized) {
    string conditionJson = R"json({
    "op" : "+",
    "params" :
    [
        200,
        200
    ]
})json";
    KhronosDataPointConditionVisitorPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryVisitLeafCondition(conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_NE(string::npos, visitor->errorInfo().find("LeafCondition pattern can not be recognized"));
}

TEST_F(KhronosDataPointConditionVisitorTest, testParseAsTimestampError_NotTimestampType) {
    string conditionJson = R"json({
        "op":">=",
        "params":[
             "$value",
             100
        ]
    })json";

    KhronosDataPointConditionVisitorPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryParseAsTimestamp(conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_EQ("[$value] is not KHRONOS_TIMESTAMP_TYPE", visitor->errorInfo());
}

TEST_F(KhronosDataPointConditionVisitorTest, testParseAsTimestampError_UnCastFailed) {
    string conditionJson = R"json({
    "op":">=",
    "params":[
        "$ts",
        {
            "op":"CAST",
            "cast_type":"integer",
            "type":"UDF",
            "params":[]
        }
    ]
})json";

    KhronosDataPointConditionVisitorPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryParseAsTimestamp(conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_NE(string::npos, visitor->errorInfo().find("unCast oprand failed:"));
}

TEST_F(KhronosDataPointConditionVisitorTest, testParseAsTimestampError_InvalidOprand) {
    string conditionJson = R"json({
        "op":">=",
        "params":[
             "$ts",
             100.1
        ]
    })json";

    KhronosDataPointConditionVisitorPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryParseAsTimestamp(conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_EQ("oprand [100.1] type is not supportd for KHRONOS_TIMESTAMP_TYPE field",
              visitor->errorInfo());
}

TEST_F(KhronosDataPointConditionVisitorTest, testParseAsTimestampError_OpNotSupport) {
    string conditionJson = R"json({
        "op":"<>",
        "params":[
             "$ts",
             100
        ]
    })json";

    KhronosDataPointConditionVisitorPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryParseAsTimestamp(conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_EQ("KHRONOS_TIMESTAMP_TYPE op [<>] not supported", visitor->errorInfo());
}

TEST_F(KhronosDataPointConditionVisitorTest, testParseAsTimestampSuccess_UnCastOprand) {
    string conditionJson = R"json({
    "op":">=",
    "params":[
        "$ts",
        {
            "op":"CAST",
            "cast_type":"integer",
            "type":"UDF",
            "params":[1546272004]
        }
    ]
})json";

    KhronosDataPointConditionVisitorPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryParseAsTimestamp(conditionJson, visitor));
    ASSERT_FALSE(visitor->isError());
    ASSERT_EQ(1546272004, visitor->_tsRange.begin);
}

TEST_F(KhronosDataPointConditionVisitorTest, testParseAsTimestampSuccess_MergeTimeRange) {
    KhronosDataPointConditionVisitor visitor(&_pool, _indexInfos);
#define VISIT_LEAF_CONDITION(conditionJson)                             \
    ASSERT_NO_FATAL_FAILURE(                                            \
            visitLeafCondition(conditionJson,                           \
                    [&] (const SimpleValue &condition) {                \
                        string op = condition[SQL_CONDITION_OPERATOR].GetString(); \
                        const SimpleValue &param = condition[SQL_CONDITION_PARAMETER]; \
                        visitor.parseAsTimestamp(param[0], param[1], op, op); \
                    }));

    {
        string conditionJson = R"json({
    "op":">=",
    "params":[
        "$ts",
        {
            "op":"CAST",
            "cast_type":"integer",
            "type":"UDF",
            "params":[1546272004]
        }
    ]
})json";
        VISIT_LEAF_CONDITION(conditionJson);
        ASSERT_FALSE(visitor.isError());
    }
    {
        string conditionJson = R"json({
    "op":">",
    "params":[
        "$ts",
        1546272005
    ]
})json";
        VISIT_LEAF_CONDITION(conditionJson);
        ASSERT_FALSE(visitor.isError());
    }
    {
        string conditionJson = R"json({
    "op":"<=",
    "params":[
        "$ts",
        1546272007
    ]
})json";
        VISIT_LEAF_CONDITION(conditionJson);
        ASSERT_FALSE(visitor.isError());
    }
    {
        string conditionJson = R"json({
    "op":"<=",
    "params":[
        "$ts",
        1546272006
    ]
})json";
        VISIT_LEAF_CONDITION(conditionJson);
        ASSERT_FALSE(visitor.isError());
    }
    ASSERT_EQ(1546272006, visitor._tsRange.begin);
    ASSERT_EQ(1546272007, visitor._tsRange.end);
#undef VISIT_LEAF_CONDITION
}

END_HA3_NAMESPACE();
