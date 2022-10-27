#include <unittest/unittest.h>
#include <autil/StringUtil.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/test/test.h>
#include <ha3/sql/ops/khronosScan/KhronosMetaConditionVisitor.h>
#include <ha3/sql/ops/condition/ExprUtil.h>
#include <ha3/sql/ops/condition/ConditionParser.h>

using namespace std;
using namespace testing;
using namespace khronos::search;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil_rapidjson;

BEGIN_HA3_NAMESPACE(sql);

class KhronosMetaConditionVisitorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
public:
    template <typename F>
    void parseToJsonAndTest(const std::string &jsonStr, F testFunc) {
        autil::mem_pool::Pool pool;
        AutilPoolAllocator allocator(&pool);
        SimpleDocument doc(&allocator);
        doc.Parse(jsonStr.c_str());
        ASSERT_FALSE(doc.HasParseError());
        ASSERT_NO_FATAL_FAILURE(testFunc(doc));
    }

    template <typename F>
    void parseLeafConditionAndTest(const std::string &conditionJson, F testFunc)
    {
        ConditionPtr cond;
        ASSERT_TRUE(_parser.parseCondition(conditionJson, cond));
        auto *leafCond = dynamic_cast<LeafCondition *>(cond.get());
        ASSERT_NE(nullptr, leafCond);
        auto &condition = leafCond->getCondition();
        ASSERT_NO_FATAL_FAILURE(testFunc(condition));
    }

    void checkParseLeafError(const string &conditionJson,
                             const string &expectedErrorInfo)
    {
        ASSERT_NO_FATAL_FAILURE(
                parseLeafConditionAndTest(conditionJson, [&] (const SimpleValue &condition) {
                            KhronosMetaConditionVisitor visitor(_indexInfos);
                            visitor.parseLeaf(condition);
                            ASSERT_TRUE(visitor.isError());
                            ASSERT_EQ(expectedErrorInfo, visitor.errorInfo());
                        }));
    }

    void checkHandleParamsError(const string &conditionJson,
                                const string &expectedErrorInfo)
    {
        ASSERT_NO_FATAL_FAILURE(
                parseLeafConditionAndTest(conditionJson, [&] (const SimpleValue &condition) {
                            string op = condition[SQL_CONDITION_OPERATOR].GetString();
                            const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
                            const SimpleValue *leftOut = nullptr;
                            const SimpleValue *rightOut = nullptr;

                            KhronosMetaConditionVisitor visitor(_indexInfos);
                            visitor.handleParams(param, op, &leftOut, &rightOut);
                            ASSERT_TRUE(visitor.isError());
                            ASSERT_EQ(expectedErrorInfo, visitor.errorInfo());
                        }));
    }

    void tryParseAsMetricSuccess(const string &conditionJson,
                                 KhronosMetaConditionVisitorPtr &visitor)
    {
        ASSERT_NO_FATAL_FAILURE(
                parseLeafConditionAndTest(conditionJson, [&] (const SimpleValue &condition) {
                            string op = condition[SQL_CONDITION_OPERATOR].GetString();
                            const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
                            ASSERT_EQ(2, param.Size());
                            visitor.reset(new KhronosMetaConditionVisitor(_indexInfos));
                            visitor->parseAsMetric(op, op, param[1]);
                            ASSERT_FALSE(visitor->isError()) << visitor->errorInfo();
                        }));
    }

    void tryParseAsMetricError(const string &conditionJson,
                               const string &expectedErrorInfo)
    {
        ASSERT_NO_FATAL_FAILURE(
                parseLeafConditionAndTest(conditionJson, [&] (const SimpleValue &condition) {
                            string op = condition[SQL_CONDITION_OPERATOR].GetString();
                            const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
                            ASSERT_EQ(2, param.Size());
                            KhronosMetaConditionVisitor visitor(_indexInfos);
                            visitor.parseAsMetric(op, op, param[1]);
                            ASSERT_TRUE(visitor.isError());
                            ASSERT_EQ(expectedErrorInfo, visitor.errorInfo());
                        }));
    }

    void tryParseMetricFromUDFWithEqualOpSuccess(
            const string &conditionJson,
            KhronosMetaConditionVisitorPtr &visitor)
    {
        ASSERT_NO_FATAL_FAILURE(
                parseLeafConditionAndTest(conditionJson, [&] (const SimpleValue &condition) {
                            string op = condition[SQL_CONDITION_OPERATOR].GetString();
                            const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
                            ASSERT_EQ(2, param.Size());
                            ASSERT_TRUE(ExprUtil::isUdf(param[1]));
                            ASSERT_EQ(SQL_EQUAL_OP, op);
                            visitor.reset(new KhronosMetaConditionVisitor(_indexInfos));
                            visitor->parseMetricFromUDFWithEqualOp(param[1]);
                            ASSERT_FALSE(visitor->isError()) << visitor->errorInfo();
                        }));
    }

    void tryParseMetricFromUDFWithEqualOpError(const string &conditionJson,
            const string &expectedErrorInfo)
    {
        ASSERT_NO_FATAL_FAILURE(
                parseLeafConditionAndTest(conditionJson, [&] (const SimpleValue &condition) {
                            string op = condition[SQL_CONDITION_OPERATOR].GetString();
                            const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
                            ASSERT_EQ(2, param.Size());
                            ASSERT_TRUE(ExprUtil::isUdf(param[1]));
                            ASSERT_EQ(SQL_EQUAL_OP, op);
                            KhronosMetaConditionVisitor visitor(_indexInfos);
                            visitor.parseMetricFromUDFWithEqualOp(param[1]);
                            ASSERT_TRUE(visitor.isError());
                            ASSERT_EQ(expectedErrorInfo, visitor.errorInfo());
                        }));
    }

    void tryParseTagkTagvFromStringSuccess(
            const string &conditionJson,
            KhronosMetaConditionVisitorPtr &visitor)
    {
        ASSERT_NO_FATAL_FAILURE(
                parseLeafConditionAndTest(conditionJson, [&] (const SimpleValue &condition) {
                            string op = condition[SQL_CONDITION_OPERATOR].GetString();
                            const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
                            ASSERT_EQ(2, param.Size());
                            ASSERT_TRUE(param[0].IsString());
                            ASSERT_TRUE(param[1].IsString());
                            visitor.reset(new KhronosMetaConditionVisitor(_indexInfos));
                            visitor->parseTagkTagvFromString(op, op,
                                    param[0].GetString(), param[1].GetString());
                            ASSERT_FALSE(visitor->isError()) << visitor->errorInfo();
                        }));
    }

    void tryParseTagkTagvFromStringError(
            const string &conditionJson,
            const string &expectedErrorInfo)
    {
        ASSERT_NO_FATAL_FAILURE(
                parseLeafConditionAndTest(conditionJson, [&] (const SimpleValue &condition) {
                            string op = condition[SQL_CONDITION_OPERATOR].GetString();
                            const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
                            ASSERT_EQ(2, param.Size());
                            ASSERT_TRUE(param[0].IsString());
                            ASSERT_TRUE(param[1].IsString());
                            KhronosMetaConditionVisitor visitor(_indexInfos);
                            visitor.parseTagkTagvFromString(op, op,
                                    param[0].GetString(), param[1].GetString());
                            ASSERT_TRUE(visitor.isError());
                            ASSERT_EQ(expectedErrorInfo, visitor.errorInfo());
                        }));
    }

private:
    IndexInfoMapType _indexInfos;
    ConditionParser _parser;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(khronosScan, KhronosMetaConditionVisitorTest);

void KhronosMetaConditionVisitorTest::setUp() {
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

void KhronosMetaConditionVisitorTest::tearDown() {
}

TEST_F(KhronosMetaConditionVisitorTest, testTs) {
}

TEST_F(KhronosMetaConditionVisitorTest, testMetricEmpty) {
    // select metric form meta where ts > 100
    string jsonStr = R"json({"op":">", "params":["$ts", 1546272100]})json";
    ConditionPtr cond;
    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_TRUE(_parser.parseCondition(jsonStr, cond));
    KhronosMetaConditionVisitor visitor(_indexInfos);
    ASSERT_NE(nullptr, cond);
    cond->accept(&visitor);
    ASSERT_FALSE(visitor.isError());
    ASSERT_EQ(0, visitor._metricVec.size());
}

TEST_F(KhronosMetaConditionVisitorTest, testMetricTs) {
    // select metric form meta where ts > 100
    string jsonStr = R"json({"op":">", "params":["$ts", 1546272100]})json";
    ConditionPtr cond;
    ASSERT_TRUE(_parser.parseCondition(jsonStr, cond));
    KhronosMetaConditionVisitor visitor(_indexInfos);
    cond->accept(&visitor);
    ASSERT_FALSE(visitor.isError());
    ASSERT_EQ(1546272101, visitor._tsRange.begin);
    ASSERT_EQ(int64_t(TimeRange::maxTime), visitor._tsRange.end);
}

TEST_F(KhronosMetaConditionVisitorTest, testMetricTsAnd) {
    // select metric form meta where 100 < ts && ts <= 200
    string jsonStr = R"json(
{
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
        }
    ]
})json";
    ConditionPtr cond;
    ASSERT_TRUE(_parser.parseCondition(jsonStr, cond));
    KhronosMetaConditionVisitor visitor(_indexInfos);
    cond->accept(&visitor);
    ASSERT_FALSE(visitor.isError());
    ASSERT_EQ(1546272101, visitor._tsRange.begin);
    ASSERT_EQ(1546272201, visitor._tsRange.end);
    ASSERT_EQ(0, visitor._metricVec.size());
    ASSERT_EQ(0, visitor._tagkVec.size());
    ASSERT_EQ(0, visitor._tagvVec.size());
}

TEST_F(KhronosMetaConditionVisitorTest, testMetricEqualOP) {
    // select metric form meta where metric='sys'
    string jsonStr = R"json({"op":"=", "params":["$metric", "sys"]})json";
    ConditionPtr cond;
    ASSERT_TRUE(_parser.parseCondition(jsonStr, cond));
    KhronosMetaConditionVisitor visitor(_indexInfos);
    cond->accept(&visitor);
    ASSERT_FALSE(visitor.isError());
    ASSERT_EQ(1, visitor._metricVec.size());
    ASSERT_EQ(SearchInterface::METRIC_PLAIN_TEXT, visitor._metricVec[0].mMetricMatchType);
    ASSERT_EQ("sys", visitor._metricVec[0].mMetricPattern);
}

TEST_F(KhronosMetaConditionVisitorTest, testMetricErrorInvalidEqualOP) {
    // select metric form meta where metric='sys*'
    string jsonStr = R"json({"op":"=", "params":["$metric", "sys*"]})json";
    ConditionPtr cond;
    ASSERT_TRUE(_parser.parseCondition(jsonStr, cond));
    KhronosMetaConditionVisitor visitor(_indexInfos);
    cond->accept(&visitor);
    ASSERT_TRUE(visitor.isError());
    ASSERT_EQ("\"*\" is not supported by metric op [=], metric [sys*]", visitor.errorInfo());
}

TEST_F(KhronosMetaConditionVisitorTest, testMetricLike) {
    // select metric form meta where metric like 'sys*'
    string jsonStr = R"json({"op":"LIKE", "params":["$metric", "sys*"]})json";
    ConditionPtr cond;
    ASSERT_TRUE(_parser.parseCondition(jsonStr, cond));
    KhronosMetaConditionVisitor visitor(_indexInfos);
    cond->accept(&visitor);
    ASSERT_FALSE(visitor.isError());
    ASSERT_EQ(int64_t(TimeRange::minTime), visitor._tsRange.begin);
    ASSERT_EQ(int64_t(TimeRange::maxTime), visitor._tsRange.end);
    ASSERT_EQ(1, visitor._metricVec.size());
    ASSERT_EQ(SearchInterface::METRIC_PREFIX, visitor._metricVec[0].mMetricMatchType);
    ASSERT_EQ("sys", visitor._metricVec[0].mMetricPattern);
    ASSERT_EQ(0, visitor._tagkVec.size());
    ASSERT_EQ(0, visitor._tagvVec.size());
}

TEST_F(KhronosMetaConditionVisitorTest, testMetricLikeOpWithoutMatchAll) {
    // select metric form meta where metric like 'sys'
    string jsonStr = R"json({"op":"LIKE", "params":["$metric", "sys"]})json";
    ConditionPtr cond;
    ASSERT_TRUE(_parser.parseCondition(jsonStr, cond));
    KhronosMetaConditionVisitor visitor(_indexInfos);
    cond->accept(&visitor);
    ASSERT_FALSE(visitor.isError());
    ASSERT_EQ(1, visitor._metricVec.size());
    ASSERT_EQ(SearchInterface::METRIC_PLAIN_TEXT, visitor._metricVec[0].mMetricMatchType);
    ASSERT_EQ("sys", visitor._metricVec[0].mMetricPattern);
}

TEST_F(KhronosMetaConditionVisitorTest, testMetricLikeOpWildcard) {
    // select metric form meta where metric like '*sys'
    string jsonStr = R"json({"op":"LIKE", "params":["$metric", "*sys"]})json";
    ConditionPtr cond;
    ASSERT_TRUE(_parser.parseCondition(jsonStr, cond));
    KhronosMetaConditionVisitor visitor(_indexInfos);
    cond->accept(&visitor);
    ASSERT_FALSE(visitor.isError());
    ASSERT_EQ(1, visitor._metricVec.size());
    ASSERT_EQ(SearchInterface::METRIC_REGEX, visitor._metricVec[0].mMetricMatchType);
    ASSERT_EQ("^.*sys$", visitor._metricVec[0].mMetricPattern);
}

TEST_F(KhronosMetaConditionVisitorTest, testMetricCombo) {
    // select metric form meta where 100 < ts AND metric like 'sys*'
    string jsonStr = R"json(
{
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
            "op" : "LIKE",
            "params" :
            [
                "$metric",
                "sys*"
            ]
        }
    ]
})json";
    ConditionPtr cond;
    ASSERT_TRUE(_parser.parseCondition(jsonStr, cond));
    KhronosMetaConditionVisitor visitor(_indexInfos);
    cond->accept(&visitor);
    ASSERT_FALSE(visitor.isError());
    ASSERT_EQ(1546272101, visitor._tsRange.begin);
    ASSERT_EQ(int64_t(TimeRange::maxTime), visitor._tsRange.end);
    ASSERT_EQ(1, visitor._metricVec.size());
    ASSERT_EQ(SearchInterface::METRIC_PREFIX, visitor._metricVec[0].mMetricMatchType);
    ASSERT_EQ("sys", visitor._metricVec[0].mMetricPattern);
    ASSERT_EQ(0, visitor._tagkVec.size());
    ASSERT_EQ(0, visitor._tagvVec.size());
}

TEST_F(KhronosMetaConditionVisitorTest, testMetricUdf) {
    {
        // select metric form meta where metric=wildcard('sys')
        string jsonStr = R"json({
    "op" : "=",
    "params" : [
        "$metric",
        {
            "op" : "wildcard",
            "params" : [
                "*sys"
            ],
            "type": "UDF"
        }
    ]
})json";
        ConditionPtr cond;
        ASSERT_TRUE(_parser.parseCondition(jsonStr, cond));
        KhronosMetaConditionVisitor visitor(_indexInfos);
        cond->accept(&visitor);
        ASSERT_FALSE(visitor.isError()) << visitor.errorInfo();
        ASSERT_EQ(1, visitor._metricVec.size());
        ASSERT_EQ(SearchInterface::METRIC_REGEX, visitor._metricVec[0].mMetricMatchType);
        ASSERT_EQ("^.*sys$", visitor._metricVec[0].mMetricPattern);
    }
    {
        // select metric form meta where metric=iwildcard('sys')
        string jsonStr = R"json({
    "op" : "=",
    "params" : [
        "$metric",
        {
            "op" : "iwildcard",
            "params" : [
                "*sys"
            ],
            "type" : "UDF"
        }
    ]
})json";
        ConditionPtr cond;
        ASSERT_TRUE(_parser.parseCondition(jsonStr, cond));
        KhronosMetaConditionVisitor visitor(_indexInfos);
        cond->accept(&visitor);
        ASSERT_FALSE(visitor.isError());
        ASSERT_EQ(1, visitor._metricVec.size());
        ASSERT_EQ(SearchInterface::METRIC_REGEX_I, visitor._metricVec[0].mMetricMatchType);
        ASSERT_EQ("^.*sys$", visitor._metricVec[0].mMetricPattern);
    }
    {
        // select metric form meta where metric=tsdb_regexp('sys')
        string jsonStr = R"json({
    "op" : "=",
    "params" : [
        "$metric",
        {
            "op" : "tsdb_regexp",
            "params" : [
                ".*sys"
            ],
            "type" : "UDF"
        }
    ]
})json";
        ConditionPtr cond;
        ASSERT_TRUE(_parser.parseCondition(jsonStr, cond));
        KhronosMetaConditionVisitor visitor(_indexInfos);
        cond->accept(&visitor);
        ASSERT_FALSE(visitor.isError());
        ASSERT_EQ(1, visitor._metricVec.size());
        ASSERT_EQ(SearchInterface::METRIC_REGEX, visitor._metricVec[0].mMetricMatchType);
        ASSERT_EQ(".*sys", visitor._metricVec[0].mMetricPattern);
    }
}

TEST_F(KhronosMetaConditionVisitorTest, testTagk) {
    // tagk cond is same as metric, will pass
}

TEST_F(KhronosMetaConditionVisitorTest, testTagv) {
    {
        // select tagv from khronos_meta where 100 < ts and tagk = 'host'
        string jsonStr = R"json(
{
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
            "op" : "=",
            "params" :
            [
                "$tagk",
                "host"
            ]
        }
    ]
})json";
        ConditionPtr cond;
        ASSERT_TRUE(_parser.parseCondition(jsonStr, cond));
        KhronosMetaConditionVisitor visitor(_indexInfos);
        cond->accept(&visitor);
        ASSERT_FALSE(visitor.isError());
        ASSERT_EQ(1546272101, visitor._tsRange.begin);
        ASSERT_EQ(int64_t(TimeRange::maxTime), visitor._tsRange.end);
        ASSERT_EQ(0, visitor._metricVec.size());
        ASSERT_EQ(1, visitor._tagkVec.size());
        ASSERT_EQ("host", visitor._tagkVec[0]);
        ASSERT_EQ(0, visitor._tagvVec.size());
    }
    {
        // select tagv from khronos_meta where ts < 100 and tagk='host' and tagv='1.1*'
        string jsonStr = R"json(
{
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
            "op" : "AND",
            "params" :
            [
                {
                    "op" : "=",
                    "params" :
                    [
                        "$tagk",
                        "host"
                    ]
                },
                {
                    "op" : "=",
                    "params" :
                    [
                        "$metric",
                        "sys.cpu"
                    ]
                }
            ]
        }
    ]
})json";
        ConditionPtr cond;
        ASSERT_TRUE(_parser.parseCondition(jsonStr, cond));
        KhronosMetaConditionVisitor visitor(_indexInfos);
        cond->accept(&visitor);
        ASSERT_FALSE(visitor.isError());
        ASSERT_EQ(1546272101, visitor._tsRange.begin);
        ASSERT_EQ(int64_t(TimeRange::maxTime), visitor._tsRange.end);
        ASSERT_EQ(1, visitor._metricVec.size());
        ASSERT_EQ(SearchInterface::METRIC_PLAIN_TEXT, visitor._metricVec[0].mMetricMatchType);
        ASSERT_EQ("sys.cpu", visitor._metricVec[0].mMetricPattern);
        ASSERT_EQ(1, visitor._tagkVec.size());
        ASSERT_EQ("host", visitor._tagkVec[0]);
        ASSERT_EQ(0, visitor._tagvVec.size());
    }
    {
        // select tagv from khronos_meta where 100 < ts and tagk = 'host' and metric = '$$sys.cpu'
        string jsonStr = R"json(
{
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
            "op" : "=",
            "params" :
            [
                "$tagk",
                "host"
            ]
        },
        {
            "op" : "=",
            "params" :
            [
                "$metric",
                "$$sys.cpu"
            ]
        }
    ]
})json";
        ConditionPtr cond;
        ASSERT_TRUE(_parser.parseCondition(jsonStr, cond));
        KhronosMetaConditionVisitor visitor(_indexInfos);
        cond->accept(&visitor);
        ASSERT_FALSE(visitor.isError());
        ASSERT_EQ(1546272101, visitor._tsRange.begin);
        ASSERT_EQ(int64_t(TimeRange::maxTime), visitor._tsRange.end);
        ASSERT_EQ(1, visitor._metricVec.size());
        ASSERT_EQ(SearchInterface::METRIC_PLAIN_TEXT, visitor._metricVec[0].mMetricMatchType);
        ASSERT_EQ("$$sys.cpu", visitor._metricVec[0].mMetricPattern);
        ASSERT_EQ(1, visitor._tagkVec.size());
        ASSERT_EQ("host", visitor._tagkVec[0]);
        ASSERT_EQ(0, visitor._tagvVec.size());
    }
    {
        // select tagv from khronos_meta where 100 < ts and tagk = 'host' and metric = 'sys.cpu' and tagv = '1.1*'
        string jsonStr = R"json(
{
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
            "op" : "=",
            "params" :
            [
                "$tagk",
                "host"
            ]
        },
        {
            "op" : "=",
            "params" :
            [
                "$metric",
                "sys.cpu"
            ]
        },
        {
            "op" : "LIKE",
            "params" :
            [
                "$tagv",
                "1.1*"
            ]
        }
    ]
})json";
        ConditionPtr cond;
        ASSERT_TRUE(_parser.parseCondition(jsonStr, cond));
        KhronosMetaConditionVisitor visitor(_indexInfos);
        cond->accept(&visitor);
        ASSERT_FALSE(visitor.isError());
        ASSERT_EQ(1546272101, visitor._tsRange.begin);
        ASSERT_EQ(int64_t(TimeRange::maxTime), visitor._tsRange.end);
        ASSERT_EQ(1, visitor._metricVec.size());
        ASSERT_EQ(SearchInterface::METRIC_PLAIN_TEXT, visitor._metricVec[0].mMetricMatchType);
        ASSERT_EQ("sys.cpu", visitor._metricVec[0].mMetricPattern);
        ASSERT_EQ(1, visitor._tagkVec.size());
        ASSERT_EQ("host", visitor._tagkVec[0]);
        ASSERT_EQ(1, visitor._tagvVec.size());
        ASSERT_EQ("1.1*", visitor._tagvVec[0]);
    }
}

TEST_F(KhronosMetaConditionVisitorTest, testParseLeafError_HandleParamsError) {
    string conditionJson = R"json({
        "op" : "=",
        "params" :["a"]
    })json";
    string expectedErrorInfo = "param [[\"a\"]] size is not 2 in op [=]";
    ASSERT_NO_FATAL_FAILURE(checkParseLeafError(conditionJson, expectedErrorInfo));
}

TEST_F(KhronosMetaConditionVisitorTest, testParseLeafError_InvalidTsValue) {
    string conditionJson = R"json({
        "op" : ">=",
        "params" :
        [
            "$ts",
            "10"
        ]
    })json";
    string expectedErrorInfo =
        "oprand [\"10\"] type is not supportd for KHRONOS_TIMESTAMP_TYPE field";
    ASSERT_NO_FATAL_FAILURE(checkParseLeafError(conditionJson, expectedErrorInfo));
}

TEST_F(KhronosMetaConditionVisitorTest, testParseLeafError_InvalidTsOp) {
    string conditionJson = R"json({
        "op" : "=",
        "params" :
        [
            "$ts",
            10
        ]
    })json";
    string expectedErrorInfo = "KHRONOS_TIMESTAMP_TYPE op [=] not supported";
    ASSERT_NO_FATAL_FAILURE(checkParseLeafError(conditionJson, expectedErrorInfo));
}

TEST_F(KhronosMetaConditionVisitorTest, testParseLeafError_InvalidTagvValue) {
    string conditionJson = R"json({
        "op" : "=",
        "params" :
        [
            "$tagv",
            10
        ]
    })json";
    string expectedErrorInfo = "oprand is not string: 10";
    ASSERT_NO_FATAL_FAILURE(checkParseLeafError(conditionJson, expectedErrorInfo));
}

TEST_F(KhronosMetaConditionVisitorTest, testParseLeafError_InvalidCond) {
    string jsonStr = R"json({})json";
    ASSERT_NO_FATAL_FAILURE(
            parseToJsonAndTest(jsonStr, [&](const SimpleValue &value) {
                            KhronosMetaConditionVisitor visitor(_indexInfos);
                            visitor.parseLeaf(value);
                            ASSERT_TRUE(visitor.isError());
                            ASSERT_EQ("leaf cond is invalid: [{}]", visitor.errorInfo());
                    }));
}

TEST_F(KhronosMetaConditionVisitorTest, testParseLeafError_InvalidTsType) {
    string conditionJson = R"json({
        "op" : ">=",
        "params" :
        [
            "$ts",
            1.1
        ]
    })json";
    string expectedErrorInfo = "oprand [1.1] type is not supportd for KHRONOS_TIMESTAMP_TYPE field";
    ASSERT_NO_FATAL_FAILURE(checkParseLeafError(conditionJson, expectedErrorInfo));
}

TEST_F(KhronosMetaConditionVisitorTest, testHandleParamsError_ParamIsNotArray) {
    string conditionJson = R"json({
        "op" : "<=",
        "params" : {}
    })json";
    string expectedErrorInfo = "param [{}] is not array in op [<=]";
    ASSERT_NO_FATAL_FAILURE(checkHandleParamsError(conditionJson, expectedErrorInfo));
}

TEST_F(KhronosMetaConditionVisitorTest, testHandleParamsError_InvalidParamSize) {
    string conditionJson = R"json({
        "op" : "<=",
        "params" : []
    })json";
    string expectedErrorInfo = "param [[]] size is not 2 in op [<=]";
    ASSERT_NO_FATAL_FAILURE(checkHandleParamsError(conditionJson, expectedErrorInfo));
}

TEST_F(KhronosMetaConditionVisitorTest, testHandleParamsError_BothColumn) {
    string conditionJson = R"json({
        "op" : "<=",
        "params" : ["$aaa","$bbb"]
    })json";
    string expectedErrorInfo = "param [[\"$aaa\",\"$bbb\"]] is invalid in op [<=]";
    ASSERT_NO_FATAL_FAILURE(checkHandleParamsError(conditionJson, expectedErrorInfo));
}

TEST_F(KhronosMetaConditionVisitorTest, testHandleParamsError_NoColumn) {
    string conditionJson = R"json({
        "op" : "<=",
        "params" : ["aaa","bbb"]
    })json";
    string expectedErrorInfo = "param [[\"aaa\",\"bbb\"]] is invalid in op [<=]";
    ASSERT_NO_FATAL_FAILURE(checkHandleParamsError(conditionJson, expectedErrorInfo));
}

TEST_F(KhronosMetaConditionVisitorTest, testHandleParamsError_InverseOpFailed) {
    string conditionJson = R"json({
        "op" : "+",
        "params" : [10,"$tagk"]
    })json";
    string expectedErrorInfo = "reverse op [+] failed";
    ASSERT_NO_FATAL_FAILURE(checkHandleParamsError(conditionJson, expectedErrorInfo));
}

TEST_F(KhronosMetaConditionVisitorTest, testHandleParamsError_UnCastFailed) {
    const string castStr = "{\"op\":\"CAST\",\"cast_type\":\"integer\",\"type\":\"UDF\",\"params\":[]}";
    string conditionJson = R"json({
        "op" : "=",
        "params" : [@cast, "$tagk"]
    })json";
    StringUtil::replaceAll(conditionJson, "@cast", castStr);
    string expectedErrorInfo = string("unCast [") + castStr + "] failed";
    ASSERT_NO_FATAL_FAILURE(checkHandleParamsError(conditionJson, expectedErrorInfo));
}

TEST_F(KhronosMetaConditionVisitorTest, testParseMetricFromUDFWithEqualOpSuccess_UnCastOp) {
    {
        string conditionJson = R"json({
    "op" : "=",
    "params" :
    [
        "$metric",
        {
            "op":"CAST",
            "cast_type":"string",
            "type":"UDF",
            "params":["sys"]
        }
    ]
})json";
        KhronosMetaConditionVisitorPtr visitor;
        ASSERT_NO_FATAL_FAILURE(tryParseMetricFromUDFWithEqualOpSuccess(conditionJson, visitor));
        auto &metricVec = visitor->_metricVec;
        ASSERT_EQ(1, metricVec.size());
        ASSERT_EQ(SearchInterface::METRIC_PLAIN_TEXT, metricVec[0].mMetricMatchType);
        ASSERT_EQ("sys", metricVec[0].mMetricPattern);
    }
    {
        string conditionJson = R"json({
    "op" : "=",
    "params" :
    [
        "$metric",
        {
            "op":"CAST",
            "cast_type":"string",
            "type":"UDF",
            "params":[
                {
                    "op":"wildcard",
                    "type":"UDF",
                    "params":["sys*"]
                }
            ]
        }
    ]
}
)json";
        KhronosMetaConditionVisitorPtr visitor;
        ASSERT_NO_FATAL_FAILURE(tryParseMetricFromUDFWithEqualOpSuccess(conditionJson, visitor));
        auto &metricVec = visitor->_metricVec;
        ASSERT_EQ(1, metricVec.size());
        ASSERT_EQ(SearchInterface::METRIC_REGEX, metricVec[0].mMetricMatchType);
        ASSERT_EQ("^sys.*$", metricVec[0].mMetricPattern);
    }
}

TEST_F(KhronosMetaConditionVisitorTest, testParseMetricFromUDFWithEqualOpSuccess_AllFunctions) {
    {
        string conditionJson = R"json({
    "op" : "=",
    "params" :
    [
        "$metric",
        {
            "op":"wildcard",
            "type":"UDF",
            "params":["sys*"]
        }
    ]
})json";
        KhronosMetaConditionVisitorPtr visitor;
        ASSERT_NO_FATAL_FAILURE(tryParseMetricFromUDFWithEqualOpSuccess(conditionJson, visitor));
        auto &metricVec = visitor->_metricVec;
        ASSERT_EQ(1, metricVec.size());
        ASSERT_EQ(SearchInterface::METRIC_REGEX, metricVec[0].mMetricMatchType);
        ASSERT_EQ("^sys.*$", metricVec[0].mMetricPattern);
    }

    {
        string conditionJson = R"json({
    "op" : "=",
    "params" :
    [
        "$metric",
        {
            "op":"iwildcard",
            "type":"UDF",
            "params":["SYS*"]
        }
    ]
})json";
        KhronosMetaConditionVisitorPtr visitor;
        ASSERT_NO_FATAL_FAILURE(tryParseMetricFromUDFWithEqualOpSuccess(conditionJson, visitor));
        auto &metricVec = visitor->_metricVec;
        ASSERT_EQ(1, metricVec.size());
        ASSERT_EQ(SearchInterface::METRIC_REGEX_I, metricVec[0].mMetricMatchType);
        ASSERT_EQ("^SYS.*$", metricVec[0].mMetricPattern);
    }

    {
        string conditionJson = R"json({
    "op" : "=",
    "params" :
    [
        "$metric",
        {
            "op":"tsdb_regexp",
            "type":"UDF",
            "params":["sys*"]
        }
    ]
})json";
        KhronosMetaConditionVisitorPtr visitor;
        ASSERT_NO_FATAL_FAILURE(tryParseMetricFromUDFWithEqualOpSuccess(conditionJson, visitor));
        auto &metricVec = visitor->_metricVec;
        ASSERT_EQ(1, metricVec.size());
        ASSERT_EQ(SearchInterface::METRIC_REGEX, metricVec[0].mMetricMatchType);
        ASSERT_EQ("sys*", metricVec[0].mMetricPattern);
    }
}

TEST_F(KhronosMetaConditionVisitorTest, testParseMetricFromUDFWithEqualOpError_TooManyParams) {
    string conditionJson = R"json({
    "op" : "=",
    "params" :
    [
        "$metric",
        {
            "op":"iwildcard",
            "type":"UDF",
            "params":["param1", "param2"]
        }
    ]
})json";
    ASSERT_NO_FATAL_FAILURE(
            tryParseMetricFromUDFWithEqualOpError(conditionJson,
                    "UDF param size must be 1 for metric [[\"param1\",\"param2\"]]"));
}

TEST_F(KhronosMetaConditionVisitorTest, testParseMetricFromUDFWithEqualOpError_ParamNotString) {
    string conditionJson = R"json({
    "op" : "=",
    "params" :
    [
        "$metric",
        {
            "op":"iwildcard",
            "type":"UDF",
            "params":[1]
        }
    ]
})json";
    ASSERT_NO_FATAL_FAILURE(
            tryParseMetricFromUDFWithEqualOpError(conditionJson,
                    "func [iwildcard] param [1] type must be string"));
}

TEST_F(KhronosMetaConditionVisitorTest, testParseMetricFromUDFWithEqualOpError_NotSupportedUDF) {
    string conditionJson = R"json({
    "op" : "=",
    "params" :
    [
        "$metric",
        {
            "op":"invalid_op",
            "type":"UDF",
            "params":["aaa"]
        }
    ]
})json";
    ASSERT_NO_FATAL_FAILURE(
            tryParseMetricFromUDFWithEqualOpError(conditionJson,
                    "UDF [invalid_op] is not supported for metric op [=]"));
}

TEST_F(KhronosMetaConditionVisitorTest, testParseAsMetricSuccess_EqualOpWithString) {
    string conditionJson = R"json({
    "op" : "=",
    "params" :
    [
        "$metric",
        "aaaa"
    ]
})json";
    KhronosMetaConditionVisitorPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryParseAsMetricSuccess(conditionJson, visitor));
    auto &metricVec = visitor->_metricVec;
    ASSERT_EQ(1, metricVec.size());
    ASSERT_EQ(SearchInterface::METRIC_PLAIN_TEXT, metricVec[0].mMetricMatchType);
    ASSERT_EQ("aaaa", metricVec[0].mMetricPattern);
}

TEST_F(KhronosMetaConditionVisitorTest, testParseAsMetricSuccess_EqualOpWithUDF) {
    string conditionJson = R"json({
    "op" : "=",
    "params" :
    [
        "$metric",
        {
            "op" : "wildcard",
            "params" : [
                "*sys"
            ],
            "type": "UDF"
        }
    ]
})json";
    KhronosMetaConditionVisitorPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryParseAsMetricSuccess(conditionJson, visitor));
    auto &metricVec = visitor->_metricVec;
    ASSERT_EQ(1, metricVec.size());
    ASSERT_EQ(SearchInterface::METRIC_REGEX, metricVec[0].mMetricMatchType);
    ASSERT_EQ("^.*sys$", metricVec[0].mMetricPattern);
}

TEST_F(KhronosMetaConditionVisitorTest, testParseAsMetricSuccess_LikeOp) {
    { // no *
        string conditionJson = R"json({
    "op" : "LIKE",
    "params" :
    [
        "$metric",
        "aaaa"
    ]
})json";
        KhronosMetaConditionVisitorPtr visitor;
        ASSERT_NO_FATAL_FAILURE(tryParseAsMetricSuccess(conditionJson, visitor));
        auto &metricVec = visitor->_metricVec;
        ASSERT_EQ(1, metricVec.size());
        ASSERT_EQ(SearchInterface::METRIC_PLAIN_TEXT, metricVec[0].mMetricMatchType);
        ASSERT_EQ("aaaa", metricVec[0].mMetricPattern);
    }

    { // only last *
        string conditionJson = R"json({
    "op" : "LIKE",
    "params" :
    [
        "$metric",
        "aaaa*"
    ]
})json";
        KhronosMetaConditionVisitorPtr visitor;
        ASSERT_NO_FATAL_FAILURE(tryParseAsMetricSuccess(conditionJson, visitor));
        auto &metricVec = visitor->_metricVec;
        ASSERT_EQ(1, metricVec.size());
        ASSERT_EQ(SearchInterface::METRIC_PREFIX, metricVec[0].mMetricMatchType);
        ASSERT_EQ("aaaa", metricVec[0].mMetricPattern);
    }

    { // only last *
        string conditionJson = R"json({
    "op" : "LIKE",
    "params" :
    [
        "$metric",
        "*"
    ]
})json";
        KhronosMetaConditionVisitorPtr visitor;
        ASSERT_NO_FATAL_FAILURE(tryParseAsMetricSuccess(conditionJson, visitor));
        auto &metricVec = visitor->_metricVec;
        ASSERT_EQ(1, metricVec.size());
        ASSERT_EQ(SearchInterface::METRIC_PREFIX, metricVec[0].mMetricMatchType);
        ASSERT_EQ("", metricVec[0].mMetricPattern);
    }

    { // multiple *
        string conditionJson = R"json({
    "op" : "LIKE",
    "params" :
    [
        "$metric",
        "a*a*aa"
    ]
})json";
        KhronosMetaConditionVisitorPtr visitor;
        ASSERT_NO_FATAL_FAILURE(tryParseAsMetricSuccess(conditionJson, visitor));
        auto &metricVec = visitor->_metricVec;
        ASSERT_EQ(1, metricVec.size());
        ASSERT_EQ(SearchInterface::METRIC_REGEX, metricVec[0].mMetricMatchType);
        ASSERT_EQ("^a.*a.*aa$", metricVec[0].mMetricPattern);
    }

    { // multiple *
        string conditionJson = R"json({
    "op" : "LIKE",
    "params" :
    [
        "$metric",
        "**"
    ]
})json";
        KhronosMetaConditionVisitorPtr visitor;
        ASSERT_NO_FATAL_FAILURE(tryParseAsMetricSuccess(conditionJson, visitor));
        auto &metricVec = visitor->_metricVec;
        ASSERT_EQ(1, metricVec.size());
        ASSERT_EQ(SearchInterface::METRIC_REGEX, metricVec[0].mMetricMatchType);
        ASSERT_EQ("^.*.*$", metricVec[0].mMetricPattern);
    }
}
TEST_F(KhronosMetaConditionVisitorTest, testParseAsMetricError_EqualOpStringHasMatchAll) {
    string conditionJson = R"json({
    "op" : "=",
    "params" :
    [
        "$metric",
        "host*"
    ]
})json";
    ASSERT_NO_FATAL_FAILURE(
            tryParseAsMetricError(conditionJson,
                    "\"*\" is not supported by metric op [=], metric [host*]"));
}

TEST_F(KhronosMetaConditionVisitorTest, testParseAsMetricError_EqualOpExprNotSupported) {
    string conditionJson = R"json({
    "op" : "=",
    "params" :
    [
        "$metric",
        {}
    ]
})json";
    ASSERT_NO_FATAL_FAILURE(
            tryParseAsMetricError(conditionJson,
                    "metric expr not supported in op [=]: [{}]"));
}

TEST_F(KhronosMetaConditionVisitorTest, testParseAsMetricError_LikeOpNotString) {
    string conditionJson = R"json({
    "op" : "LIKE",
    "params" :
    [
        "$metric",
        1
    ]
})json";
    ASSERT_NO_FATAL_FAILURE(
            tryParseAsMetricError(conditionJson,
                    "op [LIKE] only support string type oprand: [1]"));
}

TEST_F(KhronosMetaConditionVisitorTest, testParseAsMetricError_OpNotSupported) {
    string conditionJson = R"json({
    "op" : ">=",
    "params" :
    [
        "$metric",
        "host"
    ]
})json";
    ASSERT_NO_FATAL_FAILURE(
            tryParseAsMetricError(conditionJson,
                    "metric not support op [>=]"));
}

TEST_F(KhronosMetaConditionVisitorTest, testParseTagkTagvFromStringSuccess_Equal) {
    { // tagk
        string conditionJson = R"json({
    "op" : "=",
    "params" :
    [
        "$tagk",
        "host"
    ]
})json";
        KhronosMetaConditionVisitorPtr visitor;
        ASSERT_NO_FATAL_FAILURE(tryParseTagkTagvFromStringSuccess(conditionJson, visitor));
        auto &tagkVec = visitor->_tagkVec;
        ASSERT_EQ(1, tagkVec.size());
        ASSERT_EQ("host", tagkVec[0]);
    }
    { // tagv
        string conditionJson = R"json({
    "op" : "=",
    "params" :
    [
        "$tagv",
        "10.10.10.1"
    ]
})json";
        KhronosMetaConditionVisitorPtr visitor;
        ASSERT_NO_FATAL_FAILURE(tryParseTagkTagvFromStringSuccess(conditionJson, visitor));
        auto &tagvVec = visitor->_tagvVec;
        ASSERT_EQ(1, tagvVec.size());
        ASSERT_EQ("10.10.10.1", tagvVec[0]);
    }
}

TEST_F(KhronosMetaConditionVisitorTest, testParseTagkTagvFromStringSuccess_Like) {
    { // tagk
        string conditionJson = R"json({
    "op" : "LIKE",
    "params" :
    [
        "$tagk",
        "host*"
    ]
})json";
        KhronosMetaConditionVisitorPtr visitor;
        ASSERT_NO_FATAL_FAILURE(tryParseTagkTagvFromStringSuccess(conditionJson, visitor));
        auto &tagkVec = visitor->_tagkVec;
        ASSERT_EQ(1, tagkVec.size());
        ASSERT_EQ("host*", tagkVec[0]);
    }
    { // tagv
        string conditionJson = R"json({
    "op" : "LIKE",
    "params" :
    [
        "$tagv",
        "10.10.10*"
    ]
})json";
        KhronosMetaConditionVisitorPtr visitor;
        ASSERT_NO_FATAL_FAILURE(tryParseTagkTagvFromStringSuccess(conditionJson, visitor));
        auto &tagvVec = visitor->_tagvVec;
        ASSERT_EQ(1, tagvVec.size());
        ASSERT_EQ("10.10.10*", tagvVec[0]);
    }
}

TEST_F(KhronosMetaConditionVisitorTest, testParseTagkTagvFromStringError_EqualOpHasMatchAll) {
    string conditionJson = R"json({
    "op" : "=",
    "params" :
    [
        "$tagk",
        "host*"
    ]
})json";
    ASSERT_NO_FATAL_FAILURE(
            tryParseTagkTagvFromStringError(conditionJson,
                    "\"*\" is not supported by op [=], field=[$tagk], value=[host*]"));
}

TEST_F(KhronosMetaConditionVisitorTest, testParseTagkTagvFromStringError_NotSupportedOp) {
    string conditionJson = R"json({
    "op" : ">=",
    "params" :
    [
        "$tagk",
        "host"
    ]
})json";
    ASSERT_NO_FATAL_FAILURE(
            tryParseTagkTagvFromStringError(conditionJson,
                    "field [$tagk] not support op [>=]"));
}

TEST_F(KhronosMetaConditionVisitorTest, testParseTagkTagvFromStringError_NotSupportedField) {
    string conditionJson = R"json({
    "op" : "=",
    "params" :
    [
        "$invalid_field",
        "host"
    ]
})json";
    ASSERT_NO_FATAL_FAILURE(
            tryParseTagkTagvFromStringError(conditionJson,
                    "field [$invalid_field] not supported"));
}

END_HA3_NAMESPACE();
