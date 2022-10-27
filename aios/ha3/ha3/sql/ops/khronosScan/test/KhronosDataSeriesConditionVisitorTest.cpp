#include <unittest/unittest.h>
#include <autil/StringUtil.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/test/test.h>
#include <ha3/sql/ops/khronosScan/KhronosDataSeriesConditionVisitor.h>
#include <ha3/sql/ops/condition/ConditionParser.h>

using namespace std;
using namespace testing;
using namespace khronos::search;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil_rapidjson;

BEGIN_HA3_NAMESPACE(sql);

class KhronosDataSeriesConditionVisitorTest : public TESTBASE {
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
                           KhronosDataSeriesConditionVisitorPtr &visitor)
    {
        ConditionPtr cond;
        ASSERT_TRUE(_parser.parseCondition(conditionJson, cond));
        ASSERT_NE(nullptr, cond);
        visitor.reset(new KhronosDataSeriesConditionVisitor(&_pool, _indexInfos));
        cond->accept(visitor.get());
    }
    void tryVisitLeafCondition(const std::string &conditionJson,
                               KhronosDataSeriesConditionVisitorPtr &visitor)
    {
        ConditionPtr cond;
        ASSERT_TRUE(_parser.parseCondition(conditionJson, cond));
        auto *leafCond = dynamic_cast<LeafCondition *>(cond.get());
        ASSERT_NE(nullptr, leafCond);

        visitor.reset(new KhronosDataSeriesConditionVisitor(&_pool, _indexInfos));
        visitor->visitLeafCondition(leafCond);
    }
    void tryParseAsTimestampUdf(const std::string &conditionJson,
                                KhronosDataSeriesConditionVisitorPtr &visitor)
    {
        ConditionPtr cond;
        ASSERT_TRUE(_parser.parseCondition(conditionJson, cond));
        auto *leafCond = dynamic_cast<LeafCondition *>(cond.get());
        ASSERT_NE(nullptr, leafCond);
        auto &condition = leafCond->getCondition();
        visitor->parseAsTimestampUdf(condition);
    }

private:
    autil::mem_pool::Pool _pool;
    IndexInfoMapType _indexInfos;
    ConditionParser _parser;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(khronosScan, KhronosDataSeriesConditionVisitorTest);

void KhronosDataSeriesConditionVisitorTest::setUp() {
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

void KhronosDataSeriesConditionVisitorTest::tearDown() {
}

TEST_F(KhronosDataSeriesConditionVisitorTest, testVisitConditionSuccess) {
    string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "timestamp_larger_than",
            "params" :
            [
                1546272100
            ],
            "type" : "UDF"
        },
        {
            "op" : "timestamp_less_equal_than",
            "params":
            [
                1546272200
            ],
            "type" : "UDF"
        },
        {
            "op":"=",
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
        }
    ]
})json";
    KhronosDataSeriesConditionVisitorPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryVisitCondition(conditionJson, visitor));
    ASSERT_FALSE(visitor->isError()) << visitor->errorInfo();
    ASSERT_EQ(1546272101, visitor->_tsRange.begin);
    ASSERT_EQ(1546272201, visitor->_tsRange.end);
    auto &tagKVInfos = visitor->_tagKVInfos;
    ASSERT_EQ(2, tagKVInfos.size());
    ASSERT_EQ(string("host"), tagKVInfos[0].first.c_str());
    ASSERT_EQ(string("10.10*"), tagKVInfos[0].second.mTagvPattern.c_str());
    ASSERT_EQ(SearchInterface::LITERAL, tagKVInfos[0].second.mTagvMatchType);
    ASSERT_EQ(string("app"), tagKVInfos[1].first.c_str());
    ASSERT_EQ(string("aaa|bbb"), tagKVInfos[1].second.mTagvPattern.c_str());
    ASSERT_EQ(SearchInterface::LITERAL_OR, tagKVInfos[1].second.mTagvMatchType);
}

TEST_F(KhronosDataSeriesConditionVisitorTest, testVisitLeafConditionErrorParamNotArray) {
    string conditionJson = R"json({
    "op" : "<=",
    "params" : {}
})json";
    KhronosDataSeriesConditionVisitorPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryVisitLeafCondition(conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_NE(string::npos, visitor->errorInfo().find("is not array in op [<=]"));
}

TEST_F(KhronosDataSeriesConditionVisitorTest, testVisitLeafConditionError_ParamSizeNotTwo) {
    string conditionJson = R"json({
    "op" : "<=",
    "params" : []
})json";
    KhronosDataSeriesConditionVisitorPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryVisitLeafCondition(conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_NE(string::npos, visitor->errorInfo().find("size is not 2 in op [<=]"));
}

TEST_F(KhronosDataSeriesConditionVisitorTest, testParseAsTimestampUdfOpNotSupport) {
    string conditionJson = R"json({
        "op":"timestamp_not_equal",
        "params":[
             100
        ],
        "type" : "UDF"
    })json";

    KhronosDataSeriesConditionVisitorPtr visitor(
            new KhronosDataSeriesConditionVisitor(&_pool, _indexInfos));
    ASSERT_NO_FATAL_FAILURE(tryParseAsTimestampUdf(conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_EQ("udf [timestamp_not_equal] is not supported", visitor->errorInfo());
}

TEST_F(KhronosDataSeriesConditionVisitorTest, testParseAsTimestampUdfErrorParamSize) {
    string conditionJson = R"json({
        "op":"timestamp_larger_than",
        "params":[
             100,
             200
        ],
        "type" : "UDF"
    })json";

    KhronosDataSeriesConditionVisitorPtr visitor(
            new KhronosDataSeriesConditionVisitor(&_pool, _indexInfos));
    ASSERT_NO_FATAL_FAILURE(tryParseAsTimestampUdf(conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_EQ("param [[100,200]] size is not 1 in udf [timestamp_larger_than]",
              visitor->errorInfo());
}

TEST_F(KhronosDataSeriesConditionVisitorTest, testParseAsTimestampUdfErrorParamType) {
    string conditionJson = R"json({
        "op":"timestamp_larger_than",
        "params":[
             "100"
        ],
        "type" : "UDF"
    })json";

    KhronosDataSeriesConditionVisitorPtr visitor(
            new KhronosDataSeriesConditionVisitor(&_pool, _indexInfos));
    ASSERT_NO_FATAL_FAILURE(tryParseAsTimestampUdf(conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_EQ("param [[\"100\"]] must be int type in udf [timestamp_larger_than]",
              visitor->errorInfo());
}

TEST_F(KhronosDataSeriesConditionVisitorTest, testParseAsTimestampUdfSuccess) {
    KhronosDataSeriesConditionVisitorPtr visitor(
            new KhronosDataSeriesConditionVisitor(&_pool, _indexInfos));
    {
        string conditionJson = R"json({
            "op":"timestamp_larger_than",
            "params":[
                 1546272100
            ],
            "type" : "UDF"
        })json";
        ASSERT_NO_FATAL_FAILURE(tryParseAsTimestampUdf(conditionJson, visitor));
        ASSERT_FALSE(visitor->isError());
        ASSERT_EQ(1546272101, visitor->_tsRange.begin);
    }
    {
        string conditionJson = R"json({
            "op":"timestamp_larger_equal_than",
            "params":[
                 1546272110
            ],
            "type" : "UDF"
        })json";
        ASSERT_NO_FATAL_FAILURE(tryParseAsTimestampUdf(conditionJson, visitor));
        ASSERT_FALSE(visitor->isError());
        ASSERT_EQ(1546272110, visitor->_tsRange.begin);
    }
    {
        string conditionJson = R"json({
            "op":"timestamp_less_than",
            "params":[
                 1546272200
            ],
            "type" : "UDF"
        })json";
        ASSERT_NO_FATAL_FAILURE(tryParseAsTimestampUdf(conditionJson, visitor));
        ASSERT_FALSE(visitor->isError());
        ASSERT_EQ(1546272200, visitor->_tsRange.end);
    }
    {
        string conditionJson = R"json({
            "op":"timestamp_less_equal_than",
            "params":[
                 1546272190
            ],
            "type" : "UDF"
        })json";
        ASSERT_NO_FATAL_FAILURE(tryParseAsTimestampUdf(conditionJson, visitor));
        ASSERT_FALSE(visitor->isError());
        ASSERT_EQ(1546272191, visitor->_tsRange.end);
    }
    {
        // `timestamp` <= now - 1h
        string conditionJson = R"json({
            "op":"timestamp_less_equal_than",
            "params":[
                 -3600
            ],
            "type" : "UDF"
        })json";
        ASSERT_NO_FATAL_FAILURE(tryParseAsTimestampUdf(conditionJson, visitor));
        ASSERT_FALSE(visitor->isError());
        auto now = TimeUtility::currentTimeInSeconds();
        ASSERT_GE(now - 3600 + 1, visitor->_tsRange.end);
    }
}

END_HA3_NAMESPACE();
