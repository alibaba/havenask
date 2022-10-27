#include <unittest/unittest.h>
#include <autil/StringUtil.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/test/test.h>
#include <ha3/sql/ops/khronosScan/KhronosDataConditionVisitor.h>
#include <ha3/sql/ops/condition/ConditionParser.h>

using namespace std;
using namespace testing;
using namespace khronos::search;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil_rapidjson;

BEGIN_HA3_NAMESPACE(sql);

class KhronosDataConditionVisitorMock : public KhronosDataConditionVisitor {
public:
    KhronosDataConditionVisitorMock(autil::mem_pool::Pool *pool,
            const IndexInfoMapType &indexInfos)
        : KhronosDataConditionVisitor(pool, indexInfos)
    {
    }
    ~KhronosDataConditionVisitorMock() {}
public:
    void visitLeafCondition(LeafCondition *condition) override {
    }
};

HA3_TYPEDEF_PTR(KhronosDataConditionVisitorMock);

class KhronosDataConditionVisitorTest : public TESTBASE {
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
                           KhronosDataConditionVisitorMockPtr &visitor)
    {
        ConditionPtr cond;
        ASSERT_TRUE(_parser.parseCondition(conditionJson, cond));
        ASSERT_NE(nullptr, cond);
        visitor.reset(new KhronosDataConditionVisitorMock(&_pool, _indexInfos));
        cond->accept(visitor.get());
    }
    void tryParseAsItem(const std::string &conditionJson,
                        KhronosDataConditionVisitorMockPtr &visitor)
    {
        visitor.reset(new KhronosDataConditionVisitorMock(&_pool, _indexInfos));
        ASSERT_NO_FATAL_FAILURE(
                visitLeafCondition(conditionJson, [&] (const SimpleValue &condition) {
                            string op = condition[SQL_CONDITION_OPERATOR].GetString();
                            const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
                            visitor->parseAsItem(param[0], param[1], op);
                        }));
    }
    void tryParseTagsFromUDF(const std::string &tagk,
                             const std::string &conditionJson,
                             KhronosDataConditionVisitorMockPtr &visitor)
    {
        AutilPoolAllocator *allocator = new AutilPoolAllocator(&_pool);
        SimpleDocument *simpleDoc = new SimpleDocument(allocator);
        simpleDoc->Parse(conditionJson.c_str());
        visitor->parseTagsFromUDF(tagk, *simpleDoc);
        DELETE_AND_SET_NULL(simpleDoc);
        DELETE_AND_SET_NULL(allocator);
    }

private:
    autil::mem_pool::Pool _pool;
    IndexInfoMapType _indexInfos;
    ConditionParser _parser;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(khronosScan, KhronosDataConditionVisitorTest);

void KhronosDataConditionVisitorTest::setUp() {
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

void KhronosDataConditionVisitorTest::tearDown() {
}

TEST_F(KhronosDataConditionVisitorTest, testVisitAndConditionError) {
    string conditionJson = R"json({
    "op" : "AND",
    "params" :
    [
        {
            "op" : "OR",
            "params" :
            [
                {
                    "op" : "<",
                    "params" :
                    [
                        100,
                        "$ts"
                    ]
                }
            ]
        }
    ]
})json";
    KhronosDataConditionVisitorMockPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryVisitCondition(conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_EQ("[OR] cond not supported", visitor->errorInfo());
}

TEST_F(KhronosDataConditionVisitorTest, testVisitOrConditionError) {
    string conditionJson = R"json({
    "op" : "OR",
    "params" :
    [
        {
            "op" : "<",
            "params" :
            [
                100,
                "$ts"
            ]
        },
        {
            "op" : "<=",
            "params" :
            [
                "$ts",
                200
            ]
        }
    ]
})json";
    KhronosDataConditionVisitorMockPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryVisitCondition(conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_EQ("[OR] cond not supported", visitor->errorInfo());
}

TEST_F(KhronosDataConditionVisitorTest, testVisitNotConditionError) {
    string conditionJson = R"json({
    "op" : "NOT",
    "params" :
    [
        {
            "op" : "<",
            "params" :
            [
                100,
                "$ts"
            ]
        }
    ]
})json";
    KhronosDataConditionVisitorMockPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryVisitCondition(conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_EQ("[NOT] cond not supported", visitor->errorInfo());
}

TEST_F(KhronosDataConditionVisitorTest, testParseAsItemErrorNotItem) {
    string conditionJson = R"json({
        "op":"=",
        "params":[
             {},
             "10.10*"
        ],
        "type":"OTHER"
    })json";

    KhronosDataConditionVisitorMockPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryParseAsItem(conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_EQ("oprand is not ITEM type: {}", visitor->errorInfo());
}

TEST_F(KhronosDataConditionVisitorTest, testParseAsItemErrorOpNotEqual) {
    string conditionJson = R"json({
        "op":">",
        "params":[
             {
                 "op":"ITEM",
                 "params":[
                     "$tags",
                     "host"
                 ],
                 "type":"OTHER"
             },
             "10.10*"
        ],
        "type":"OTHER"
    })json";

    KhronosDataConditionVisitorMockPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryParseAsItem(conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_EQ("tags condition only support [= or like] operator: tagk=[host], op=[>]",
              visitor->errorInfo());
}

TEST_F(KhronosDataConditionVisitorTest, testParseAsItemErrorWrongValueType) {
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
             1
        ],
        "type":"OTHER"
    })json";

    KhronosDataConditionVisitorMockPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryParseAsItem(conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_EQ("oprand type can not be recognized for tagk=[host]: 1",
              visitor->errorInfo());
}

TEST_F(KhronosDataConditionVisitorTest, testParseAsItemSuccessUseLike) {
    string conditionJson = R"json({
        "op":"LIKE",
        "params":[
             {
                 "op":"ITEM",
                 "params":[
                     "$tags",
                     "host"
                 ],
                 "type":"OTHER"
             },
             "10.10*"
        ],
        "type":"OTHER"
    })json";

    KhronosDataConditionVisitorMockPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryParseAsItem(conditionJson, visitor));
    ASSERT_FALSE(visitor->isError());
    auto &tagKVInfos = visitor->_tagKVInfos;
    ASSERT_EQ(1, tagKVInfos.size());
    ASSERT_EQ(string("host"), tagKVInfos[0].first.c_str());
    ASSERT_EQ(string("10.10*"), tagKVInfos[0].second.mTagvPattern.c_str());
    ASSERT_EQ(SearchInterface::LITERAL, tagKVInfos[0].second.mTagvMatchType);
}

TEST_F(KhronosDataConditionVisitorTest, testParseAsItemSuccessUseEqual) {
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
             "10.10*"
        ],
        "type":"OTHER"
    })json";

    KhronosDataConditionVisitorMockPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryParseAsItem(conditionJson, visitor));
    ASSERT_FALSE(visitor->isError());
    auto &tagKVInfos = visitor->_tagKVInfos;
    ASSERT_EQ(1, tagKVInfos.size());
    ASSERT_EQ(string("host"), tagKVInfos[0].first.c_str());
    ASSERT_EQ(string("10.10*"), tagKVInfos[0].second.mTagvPattern.c_str());
    ASSERT_EQ(SearchInterface::LITERAL, tagKVInfos[0].second.mTagvMatchType);
}

TEST_F(KhronosDataConditionVisitorTest, testParseAsItemSuccessUseUdf) {
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

    KhronosDataConditionVisitorMockPtr visitor;
    ASSERT_NO_FATAL_FAILURE(tryParseAsItem(conditionJson, visitor));
    ASSERT_FALSE(visitor->isError());
    auto &tagKVInfos = visitor->_tagKVInfos;
    ASSERT_EQ(1, tagKVInfos.size());
    ASSERT_EQ(string("host"), tagKVInfos[0].first.c_str());
    ASSERT_EQ(string("10.10*"), tagKVInfos[0].second.mTagvPattern.c_str());
    ASSERT_EQ(SearchInterface::LITERAL_OR, tagKVInfos[0].second.mTagvMatchType);
}

TEST_F(KhronosDataConditionVisitorTest, testParseTagsFromUDF) {
    vector<string> conditionJsonVec;
    vector<string> tagkVec;
    vector<SearchInterface::TAGV_MATCH_TYPE> matchTypeVec;
    vector<string> tagvVec;
    {
        // case 1
        conditionJsonVec.emplace_back(R"json({
            "op":"literal_or",
            "params":["10"],
            "type":"UDF"
        })json");
        tagkVec.emplace_back("host1");
        matchTypeVec.emplace_back(SearchInterface::LITERAL_OR);
        tagvVec.emplace_back("10");
    }
    {
        // case 2
        conditionJsonVec.emplace_back(R"json({
            "op":"iliteral_or",
            "params":["10"],
            "type":"UDF"
        })json");
        tagkVec.emplace_back("host2");
        matchTypeVec.emplace_back(SearchInterface::ILITERAL_OR);
        tagvVec.emplace_back("10");
    }
    {
        // case 3
        conditionJsonVec.emplace_back(R"json({
            "op":"not_literal_or",
            "params":["10"],
            "type":"UDF"
        })json");
        tagkVec.emplace_back("host3");
        matchTypeVec.emplace_back(SearchInterface::NOT_LITERAL_OR);
        tagvVec.emplace_back("10");
    }
    {
        // case 4
        conditionJsonVec.emplace_back(R"json({
            "op":"not_iliteral_or",
            "params":["10"],
            "type":"UDF"
        })json");
        tagkVec.emplace_back("host4");
        matchTypeVec.emplace_back(SearchInterface::NOT_ILITERAL_OR);
        tagvVec.emplace_back("10");
    }
    {
        // case 5
        conditionJsonVec.emplace_back(R"json({
            "op":"wildcard",
            "params":["**10"],
            "type":"UDF"
        })json");
        tagkVec.emplace_back("host5");
        matchTypeVec.emplace_back(SearchInterface::WILD_CARD);
        tagvVec.emplace_back("**10");
    }
    {
        // case 6
        conditionJsonVec.emplace_back(R"json({
            "op":"iwildcard",
            "params":["**10"],
            "type":"UDF"
        })json");
        tagkVec.emplace_back("host6");
        matchTypeVec.emplace_back(SearchInterface::WILD_CARD_I);
        tagvVec.emplace_back("**10");
    }
    {
        // case 7
        conditionJsonVec.emplace_back(R"json({
            "op":"tsdb_regexp",
            "params":["10"],
            "type":"UDF"
        })json");
        tagkVec.emplace_back("host7");
        matchTypeVec.emplace_back(SearchInterface::REGEX);
        tagvVec.emplace_back("10");
    }

    KhronosDataConditionVisitorMockPtr visitor(
            new KhronosDataConditionVisitorMock(&_pool, _indexInfos));
    for (size_t i = 0; i < 7; ++i) {
        ASSERT_NO_FATAL_FAILURE(tryParseTagsFromUDF(tagkVec[i], conditionJsonVec[i], visitor));
        size_t caseId = i + 1;
        ASSERT_FALSE(visitor->isError()) << visitor->errorInfo() << " case :" << caseId;
        auto &tagKVInfos = visitor->_tagKVInfos;
        ASSERT_EQ(caseId, tagKVInfos.size()) << " case :" << caseId;
        ASSERT_EQ(tagkVec[i], tagKVInfos[i].first.c_str()) << " case :" << caseId;
        ASSERT_EQ(tagvVec[i], tagKVInfos[i].second.mTagvPattern.c_str()) << " case :" << caseId;
        ASSERT_EQ(matchTypeVec[i], tagKVInfos[i].second.mTagvMatchType) << " case :" << caseId;
    }
}

TEST_F(KhronosDataConditionVisitorTest, testParseTagsFromUDFErrorOp) {
    string conditionJson = (R"json({
        "op":"literal_and",
        "params":["10"],
        "type":"UDF"
    })json");
    KhronosDataConditionVisitorMockPtr visitor(
            new KhronosDataConditionVisitorMock(&_pool, _indexInfos));
    ASSERT_NO_FATAL_FAILURE(tryParseTagsFromUDF("host", conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_EQ("UDF [literal_and] is not supported for tagk=[host] tagv=[10]", visitor->errorInfo());
}

TEST_F(KhronosDataConditionVisitorTest, testParseTagsFromUDFNotUdf) {
    string conditionJson = (R"json({
        "op":"literal_or",
        "params":["10"],
        "type":"xxx"
    })json");
    KhronosDataConditionVisitorMockPtr visitor(
            new KhronosDataConditionVisitorMock(&_pool, _indexInfos));
    ASSERT_NO_FATAL_FAILURE(tryParseTagsFromUDF("host", conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_EQ("not udf for tagk=[host], value is {\"op\":\"literal_or\",\"params\":[\"10\"],\"type\":\"xxx\"}", visitor->errorInfo());
}

TEST_F(KhronosDataConditionVisitorTest, testParseTagsFromUDFWrongParam) {
    string conditionJson = (R"json({
        "op":"literal_or",
        "params":[10],
        "type":"UDF"
    })json");
    KhronosDataConditionVisitorMockPtr visitor(
            new KhronosDataConditionVisitorMock(&_pool, _indexInfos));
    ASSERT_NO_FATAL_FAILURE(tryParseTagsFromUDF("host", conditionJson, visitor));
    ASSERT_TRUE(visitor->isError());
    ASSERT_EQ("func [literal_or] param [10] type must be string", visitor->errorInfo());
}

TEST_F(KhronosDataConditionVisitorTest, testParseTagsFromUDFMatchAll) {
    string conditionJson = (R"json({
        "op":"literal_or",
        "params":["*"],
        "type":"UDF"
    })json");
    KhronosDataConditionVisitorMockPtr visitor(
            new KhronosDataConditionVisitorMock(&_pool, _indexInfos));
    ASSERT_NO_FATAL_FAILURE(tryParseTagsFromUDF("host", conditionJson, visitor));
    ASSERT_FALSE(visitor->isError());
    auto &tagKVInfos = visitor->_tagKVInfos;
    ASSERT_EQ(1, tagKVInfos.size());
}

END_HA3_NAMESPACE();
