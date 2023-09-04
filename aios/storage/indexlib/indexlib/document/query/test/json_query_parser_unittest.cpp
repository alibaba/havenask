#include "indexlib/common_define.h"
#include "indexlib/document/query/and_query.h"
#include "indexlib/document/query/json_query_parser.h"
#include "indexlib/document/query/not_query.h"
#include "indexlib/document/query/or_query.h"
#include "indexlib/document/query/pattern_query.h"
#include "indexlib/document/query/query_function.h"
#include "indexlib/document/query/range_query.h"
#include "indexlib/document/query/sub_term_query.h"
#include "indexlib/document/query/term_query.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace indexlib::file_system;

namespace indexlib { namespace document {

class JsonQueryParserTest : public INDEXLIB_TESTBASE
{
public:
    JsonQueryParserTest();
    ~JsonQueryParserTest();
    DECLARE_CLASS_NAME(JsonQueryParserTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestFunction();
    void TestQueryWithFunction();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(JsonQueryParserTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(JsonQueryParserTest, TestFunction);
INDEXLIB_UNIT_TEST_CASE(JsonQueryParserTest, TestQueryWithFunction);

IE_LOG_SETUP(document, JsonQueryParserTest);

JsonQueryParserTest::JsonQueryParserTest() {}

JsonQueryParserTest::~JsonQueryParserTest() {}

void JsonQueryParserTest::CaseSetUp() {}

void JsonQueryParserTest::CaseTearDown() {}

void JsonQueryParserTest::TestSimpleProcess()
{
    string queryStr = R"(
{
    "type" : "AND",
    "sub_query" : [
        { "type" : "RANGE", "field" : "range_field", "value" : "(-183, 257]" },
        { "type" : "TERM", "field" : "term_field", "value" : "hello world"},
        { 
           "type" : "OR", 
           "sub_query" : [
               { 
                   "type" : "NOT", 
                   "sub_query" :  [
                       { "type" : "SUB_TERM", "field" : "categary", "value" : "iphone" }
                   ]
               },
               {
                   "type" : "RANGE", "field" : "double_field", "value" : "[16.78, 100]"
               }
           ]
        },
        { "type" : "PATTERN", "field" : "pattern_field", "pattern" : "*pattern*"},
        {
            "type" : "NOT",
            "sub_query" :  [
                { "type" : "RANGE", "field" : "number2", "value" : "666" }
            ]
        }
    ]
}
)";

    JsonQueryParser parser;
    QueryBasePtr query = parser.Parse(queryStr);
    string jsonStr = ToJsonString(query, true);
    cout << jsonStr << endl;
    query = parser.Parse(jsonStr);

    ASSERT_EQ(QT_AND, query->GetType());
    AndQueryPtr andQuery = DYNAMIC_POINTER_CAST(AndQuery, query);
    const vector<QueryBasePtr>& subQuery = andQuery->GetSubQuery();
    ASSERT_EQ(5, subQuery.size());

    ASSERT_EQ(QT_RANGE, subQuery[0]->GetType());
    RangeQueryPtr rangeQuery = DYNAMIC_POINTER_CAST(RangeQuery, subQuery[0]);
    ASSERT_EQ(string("range_field"), rangeQuery->GetFieldName());
    auto rangeInfo = rangeQuery->GetRangeInfo();
    ASSERT_EQ(string("-183"), rangeInfo.leftStr);
    ASSERT_EQ(string("257"), rangeInfo.rightStr);
    ASSERT_FALSE(rangeInfo.leftInclude);
    ASSERT_TRUE(rangeInfo.rightInclude);
    ASSERT_EQ(-183, rangeInfo.leftValue.intValue);
    ASSERT_EQ(257, rangeInfo.rightValue.intValue);
    ASSERT_EQ(RangeQuery::RVT_INT, rangeInfo.valueType);

    ASSERT_EQ(QT_TERM, subQuery[1]->GetType());
    TermQueryPtr termQuery = DYNAMIC_POINTER_CAST(TermQuery, subQuery[1]);
    ASSERT_EQ(string("term_field"), termQuery->GetFieldName());
    ASSERT_EQ(string("hello world"), termQuery->GetValue());

    ASSERT_EQ(QT_OR, subQuery[2]->GetType());
    OrQueryPtr orQuery = DYNAMIC_POINTER_CAST(OrQuery, subQuery[2]);

    const vector<QueryBasePtr>& orSubQuery = orQuery->GetSubQuery();
    ASSERT_EQ(2, orSubQuery.size());

    ASSERT_EQ(QT_NOT, orSubQuery[0]->GetType());
    NotQueryPtr notQuery = DYNAMIC_POINTER_CAST(NotQuery, orSubQuery[0]);

    {
        const vector<QueryBasePtr> notSubQuery = notQuery->GetSubQuery();
        ASSERT_EQ(1, notSubQuery.size());
        ASSERT_EQ(QT_SUB_TERM, notSubQuery[0]->GetType());
        SubTermQueryPtr subTermQuery = DYNAMIC_POINTER_CAST(SubTermQuery, notSubQuery[0]);
        ASSERT_EQ(string("categary"), subTermQuery->GetFieldName());
        ASSERT_EQ(string("iphone"), subTermQuery->GetSubValue());
    }

    rangeQuery = DYNAMIC_POINTER_CAST(RangeQuery, orSubQuery[1]);
    ASSERT_EQ(string("double_field"), rangeQuery->GetFieldName());
    rangeInfo = rangeQuery->GetRangeInfo();
    ASSERT_EQ(RangeQuery::RVT_DOUBLE, rangeInfo.valueType);
    ASSERT_TRUE(rangeInfo.leftInclude);
    ASSERT_TRUE(rangeInfo.rightInclude);
    ASSERT_EQ(16.78, rangeInfo.leftValue.doubleValue);
    ASSERT_EQ(100, rangeInfo.rightValue.doubleValue);

    ASSERT_EQ(QT_PATTERN, subQuery[3]->GetType());
    PatternQueryPtr patternQuery = DYNAMIC_POINTER_CAST(PatternQuery, subQuery[3]);
    ASSERT_EQ(string("pattern_field"), patternQuery->GetFieldName());
    ASSERT_EQ(string("*pattern*"), patternQuery->GetFieldPattern());

    ASSERT_EQ(QT_NOT, subQuery[4]->GetType());
    notQuery = DYNAMIC_POINTER_CAST(NotQuery, subQuery[4]);
    const vector<QueryBasePtr>& notSubQuery = notQuery->GetSubQuery();
    ASSERT_EQ(1, notSubQuery.size());
    ASSERT_EQ(QT_RANGE, notSubQuery[0]->GetType());
    RangeQueryPtr rangeQuery2 = DYNAMIC_POINTER_CAST(RangeQuery, notSubQuery[0]);
    ASSERT_EQ(string("number2"), rangeQuery2->GetFieldName());
    auto rangeInfo2 = rangeQuery2->GetRangeInfo();
    ASSERT_EQ(RangeQuery::RVT_UINT, rangeInfo2.valueType);
    ASSERT_TRUE(rangeInfo2.leftInclude);
    ASSERT_TRUE(rangeInfo2.rightInclude);
    ASSERT_EQ(666, rangeInfo2.leftValue.uintValue);
    ASSERT_EQ(666, rangeInfo2.rightValue.uintValue);
}

void JsonQueryParserTest::TestQueryWithFunction()
{
    string queryFilePath = GET_PRIVATE_TEST_DATA_PATH() + "/query_function.json";
    string queryStr;
    FslibWrapper::AtomicLoadE(queryFilePath, queryStr);

    JsonQueryParser parser;
    QueryBasePtr query = parser.Parse(queryStr);
    string jsonStr = ToJsonString(query, true);
    cout << jsonStr << endl;
    query = parser.Parse(jsonStr);

    ASSERT_EQ(QT_AND, query->GetType());
    AndQueryPtr andQuery = DYNAMIC_POINTER_CAST(AndQuery, query);
    const vector<QueryBasePtr>& subQuery = andQuery->GetSubQuery();
    ASSERT_EQ(4, subQuery.size());

    ASSERT_EQ(QT_RANGE, subQuery[0]->GetType());
    RangeQueryPtr rangeQuery = DYNAMIC_POINTER_CAST(RangeQuery, subQuery[0]);
    ASSERT_TRUE(rangeQuery->IsFunctionQuery());
    ASSERT_EQ(string("func1_value"), rangeQuery->GetFunction().GetAliasField());
    ASSERT_EQ(string("func1_value"), rangeQuery->GetFunction().GetFunctionKey());

    ASSERT_EQ(QT_TERM, subQuery[1]->GetType());
    TermQueryPtr termQuery = DYNAMIC_POINTER_CAST(TermQuery, subQuery[1]);
    ASSERT_TRUE(termQuery->IsFunctionQuery());
    ASSERT_EQ(string("func2_value"), termQuery->GetFunction().GetAliasField());
    ASSERT_EQ(string("func2_value"), termQuery->GetFunction().GetFunctionKey());

    ASSERT_EQ(QT_SUB_TERM, subQuery[2]->GetType());
    SubTermQueryPtr subTermQuery = DYNAMIC_POINTER_CAST(SubTermQuery, subQuery[2]);
    ASSERT_TRUE(subTermQuery->IsFunctionQuery());
    ASSERT_EQ(string(""), subTermQuery->GetFunction().GetAliasField());
    ASSERT_NE(string(""), subTermQuery->GetFunction().GetFunctionKey());

    ASSERT_EQ(QT_PATTERN, subQuery[3]->GetType());
    PatternQueryPtr patternQuery = DYNAMIC_POINTER_CAST(PatternQuery, subQuery[3]);
    ASSERT_TRUE(patternQuery->IsFunctionQuery());
    ASSERT_EQ(string("func4_value"), patternQuery->GetFunction().GetAliasField());
    ASSERT_EQ(string("func4_value"), patternQuery->GetFunction().GetFunctionKey());
}

void JsonQueryParserTest::TestFunction()
{
    QueryFunction function("udf_func");
    function.AddFunctionParam("path", "hdfs://123.23.22,14/abcd");
    function.AddFunctionParam("user", "pujian");
    function.AddFunctionParam("", "456");

    string str = function.ToString();
    cout << str << endl;

    QueryFunction newFunction;
    ASSERT_TRUE(newFunction.FromString(str));

    ASSERT_EQ(string("udf_func"), newFunction.GetFunctionName());
    ASSERT_EQ(3, newFunction.GetFunctionParamNum());

    auto funcParams = newFunction.GetFunctionParameters();
    ASSERT_EQ(string("path"), funcParams[0].first);
    ASSERT_EQ(string("hdfs://123.23.22,14/abcd"), funcParams[0].second);
    ASSERT_EQ(string("user"), funcParams[1].first);
    ASSERT_EQ(string("pujian"), funcParams[1].second);
    ASSERT_EQ(string(""), funcParams[2].first);
    ASSERT_EQ(string("456"), funcParams[2].second);

    string param;
    ASSERT_TRUE(newFunction.GetFunctionParam("path", param));
    ASSERT_EQ(string("hdfs://123.23.22,14/abcd"), param);

    ASSERT_TRUE(newFunction.GetFunctionParam("user", param));
    ASSERT_EQ(string("pujian"), param);

    ASSERT_FALSE(newFunction.GetFunctionParam("not-exist", param));
    ASSERT_FALSE(newFunction.GetFunctionParam("", param));
}
}} // namespace indexlib::document
