#include "indexlib/common_define.h"
#include "indexlib/document/query/json_query_parser.h"
#include "indexlib/document/query/query_evaluator_creator.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace document {

class QueryEvaluatorTest : public INDEXLIB_TESTBASE
{
public:
    QueryEvaluatorTest();
    ~QueryEvaluatorTest();

    DECLARE_CLASS_NAME(QueryEvaluatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestTermQueryEvaluator();
    void TestAndQueryEvaluator();
    void TestOrQueryEvaluator();
    void TestNotQueryEvaluator();
    void TestSubTermQueryEvaluator();
    void TestRangeQueryEvaluator();
    void TestPatternQueryEvaluator();
    void TestQueryEvaluatorWithFunction();

    RawDocumentPtr CreateRawDocument(const std::map<std::string, std::string>& kvMap);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(QueryEvaluatorTest, TestTermQueryEvaluator);
INDEXLIB_UNIT_TEST_CASE(QueryEvaluatorTest, TestAndQueryEvaluator);
INDEXLIB_UNIT_TEST_CASE(QueryEvaluatorTest, TestOrQueryEvaluator);
INDEXLIB_UNIT_TEST_CASE(QueryEvaluatorTest, TestNotQueryEvaluator);
INDEXLIB_UNIT_TEST_CASE(QueryEvaluatorTest, TestSubTermQueryEvaluator);
INDEXLIB_UNIT_TEST_CASE(QueryEvaluatorTest, TestRangeQueryEvaluator);
INDEXLIB_UNIT_TEST_CASE(QueryEvaluatorTest, TestPatternQueryEvaluator);
INDEXLIB_UNIT_TEST_CASE(QueryEvaluatorTest, TestQueryEvaluatorWithFunction);

IE_LOG_SETUP(document, QueryEvaluatorTest);

QueryEvaluatorTest::QueryEvaluatorTest() {}

QueryEvaluatorTest::~QueryEvaluatorTest() {}

void QueryEvaluatorTest::CaseSetUp() {}

void QueryEvaluatorTest::CaseTearDown() {}

void QueryEvaluatorTest::TestTermQueryEvaluator()
{
    string queryStr = R"(
{
    "type" : "TERM",
    "field" : "field2",
    "value" : "hello world"
}
)";

    JsonQueryParser parser;
    QueryBasePtr query = parser.Parse(queryStr);

    QueryEvaluatorCreator creator;
    QueryEvaluatorPtr evaluator = creator.Create(query);

    RawDocumentPtr rawDoc = CreateRawDocument({{"field1", "200"}, {"field2", "hello world"}});
    ASSERT_EQ(ES_TRUE, evaluator->Evaluate(rawDoc));

    rawDoc = CreateRawDocument({{"field1", "-183"}, {"field2", "hello world"}});
    ASSERT_EQ(ES_TRUE, evaluator->Evaluate(rawDoc));

    rawDoc = CreateRawDocument({{"field1", "-100"}, {"field2", "hello world1"}});
    ASSERT_EQ(ES_FALSE, evaluator->Evaluate(rawDoc));
}

void QueryEvaluatorTest::TestAndQueryEvaluator()
{
    string queryStr = R"(
{
    "type" : "AND",
    "sub_query" : [
        { "type" : "RANGE", "field" : "field1", "value" : "(-183, 257]" },
        { "type" : "TERM", "field" : "field2", "value" : "hello world"}
    ]
}
)";

    JsonQueryParser parser;
    QueryBasePtr query = parser.Parse(queryStr);

    QueryEvaluatorCreator creator;
    QueryEvaluatorPtr evaluator = creator.Create(query);

    RawDocumentPtr rawDoc = CreateRawDocument({{"field1", "200"}, {"field2", "hello world"}});
    ASSERT_EQ(ES_TRUE, evaluator->Evaluate(rawDoc));

    rawDoc = CreateRawDocument({{"field1", "-183"}, {"field2", "hello world"}});
    ASSERT_EQ(ES_FALSE, evaluator->Evaluate(rawDoc));

    rawDoc = CreateRawDocument({{"field1", "-100"}, {"field2", "hello world1"}});
    ASSERT_EQ(ES_FALSE, evaluator->Evaluate(rawDoc));
}

void QueryEvaluatorTest::TestOrQueryEvaluator()
{
    string queryStr = R"(
{
    "type" : "OR",
    "sub_query" : [
        { "type" : "RANGE", "field" : "field1", "value" : "(-183, 257]" },
        { "type" : "TERM", "field" : "field2", "value" : "hello world"}
    ]
}
)";

    JsonQueryParser parser;
    QueryBasePtr query = parser.Parse(queryStr);

    QueryEvaluatorCreator creator;
    QueryEvaluatorPtr evaluator = creator.Create(query);

    RawDocumentPtr rawDoc = CreateRawDocument({{"field1", "200"}, {"field2", "hello world"}});
    ASSERT_EQ(ES_TRUE, evaluator->Evaluate(rawDoc));

    rawDoc = CreateRawDocument({{"field1", "-183"}, {"field2", "hello world"}});
    ASSERT_EQ(ES_TRUE, evaluator->Evaluate(rawDoc));

    rawDoc = CreateRawDocument({{"field1", "-100"}, {"field2", "hello world1"}});
    ASSERT_EQ(ES_TRUE, evaluator->Evaluate(rawDoc));

    rawDoc = CreateRawDocument({{"field1", "-183"}, {"field2", "hello world1"}});
    ASSERT_EQ(ES_FALSE, evaluator->Evaluate(rawDoc));
}

void QueryEvaluatorTest::TestNotQueryEvaluator()
{
    string queryStr = R"(
{
    "type" : "NOT",
    "sub_query" : [
        { "type" : "TERM", "field" : "field2", "value" : "hello world"}
    ]
}
)";

    JsonQueryParser parser;
    QueryBasePtr query = parser.Parse(queryStr);

    QueryEvaluatorCreator creator;
    QueryEvaluatorPtr evaluator = creator.Create(query);

    RawDocumentPtr rawDoc = CreateRawDocument({{"field1", "200"}, {"field2", "hello world"}});
    ASSERT_EQ(ES_FALSE, evaluator->Evaluate(rawDoc));

    rawDoc = CreateRawDocument({{"field1", "-183"}, {"field2", "hello world1"}});
    ASSERT_EQ(ES_TRUE, evaluator->Evaluate(rawDoc));

    rawDoc = CreateRawDocument({{"field1", "-100"}});
    QueryEvaluator::EvaluateParam param;
    param.pendingForFieldNotExist = true;
    ASSERT_EQ(ES_PENDING, evaluator->Evaluate(rawDoc, param));
    ASSERT_EQ(ES_TRUE, evaluator->Evaluate(rawDoc));
}

void QueryEvaluatorTest::TestRangeQueryEvaluator()
{
    string queryStr = "{\"type\" : \"RANGE\",\"field\" : \"field1\",\"value\" : \"[-100, 120)\"}";

    JsonQueryParser parser;
    QueryBasePtr query = parser.Parse(queryStr);

    QueryEvaluatorCreator creator;
    QueryEvaluatorPtr evaluator = creator.Create(query);

    RawDocumentPtr rawDoc = CreateRawDocument({{"field1", "-100"}});
    ASSERT_EQ(ES_TRUE, evaluator->Evaluate(rawDoc));

    rawDoc = CreateRawDocument({{"field1", "120"}});
    ASSERT_EQ(ES_FALSE, evaluator->Evaluate(rawDoc));

    rawDoc = CreateRawDocument({{"field1", "130"}});
    ASSERT_EQ(ES_FALSE, evaluator->Evaluate(rawDoc));
}

void QueryEvaluatorTest::TestSubTermQueryEvaluator()
{
    string queryStr = R"(
{
    "type" : "SUB_TERM",
    "field" : "field1",
    "value" : "abcd",
    "delimeter" : ","
}
)";

    JsonQueryParser parser;
    QueryBasePtr query = parser.Parse(queryStr);

    QueryEvaluatorCreator creator;
    QueryEvaluatorPtr evaluator = creator.Create(query);

    RawDocumentPtr rawDoc = CreateRawDocument({{"field1", "aaaa,abc,abcd"}});
    ASSERT_EQ(ES_TRUE, evaluator->Evaluate(rawDoc));

    rawDoc = CreateRawDocument({{"field1", "abc,bcd,acd,cda,bcd"}});
    ASSERT_EQ(ES_FALSE, evaluator->Evaluate(rawDoc));
}

void QueryEvaluatorTest::TestPatternQueryEvaluator()
{
    string queryStr = R"(
{
    "type" : "AND",
    "sub_query" : [
        { "type" : "RANGE", "field" : "field1", "value" : "(-183, 257]" },
        { "type" : "SUB_TERM", "field" : "field2", "value" : "hello", "delimeter" : ":"},
        { "type" : "PATTERN", "field" : "field3", "pattern" : ".*abc.*"}
    ]
}
)";

    JsonQueryParser parser;
    QueryBasePtr query = parser.Parse(queryStr);

    QueryEvaluatorCreator creator;
    QueryEvaluatorPtr evaluator = creator.Create(query);

    RawDocumentPtr rawDoc = CreateRawDocument({{"field1", "200"}, {"field2", "hello:world"}, {"field3", "test abcd"}});
    ASSERT_EQ(ES_TRUE, evaluator->Evaluate(rawDoc));

    rawDoc = CreateRawDocument({{"field1", "200"}, {"field2", "hello:world"}, {"field3", "test abd"}});
    ASSERT_EQ(ES_FALSE, evaluator->Evaluate(rawDoc));
}

void QueryEvaluatorTest::TestQueryEvaluatorWithFunction()
{
    string queryFilePath = GET_PRIVATE_TEST_DATA_PATH() + "/query_function.json";
    string queryStr;
    FslibWrapper::AtomicLoadE(queryFilePath, queryStr);

    JsonQueryParser parser;
    QueryBasePtr query = parser.Parse(queryStr);

    string pluginPath = util::PathUtil::JoinPath(GET_PRIVATE_TEST_DATA_PATH(), "plugins");

    config::FunctionExecutorResource resource;
    config::ModuleInfo info;
    info.moduleName = "test_module";
    info.modulePath = "libexample_function_executor.so";
    resource.moduleInfos.push_back(info);

    config::FunctionExecutorParam param;
    param.functionName = "plus";
    param.moduleName = "test_module";
    resource.functionParams.push_back(param);
    param.functionName = "append";
    resource.functionParams.push_back(param);

    QueryEvaluatorCreator creator;
    ASSERT_TRUE(creator.Init(pluginPath, resource));

    QueryEvaluatorPtr evaluator = creator.Create(query);
    RawDocumentPtr rawDoc =
        CreateRawDocument({{"f1", "100"}, {"f2", "hello world"}, {"f3", "hello abcd"}, {"f4", "patt"}});
    ASSERT_EQ(ES_TRUE, evaluator->Evaluate(rawDoc));

    ASSERT_EQ(string("120"), rawDoc->getField("func1_value"));
    ASSERT_EQ(string("hello world123"), rawDoc->getField("func2_value"));
    ASSERT_EQ(string("pattern"), rawDoc->getField("func4_value"));

    vector<autil::StringView> fieldNames;
    rawDoc->extractFieldNames(fieldNames);
    for (auto& fn : fieldNames) {
        if (fn.find("__FUNC__:") == 0) {
            string name = string(fn.data(), fn.size());
            string value = rawDoc->getField(name);
            cout << name << "=" << value << endl;
            ASSERT_EQ(string("hello abcd456"), value);
        }
    }
}

RawDocumentPtr QueryEvaluatorTest::CreateRawDocument(const map<string, string>& kvMap)
{
    RawDocumentPtr rawDoc(new DefaultRawDocument());
    for (auto& item : kvMap) {
        rawDoc->setField(item.first, item.second);
    }
    return rawDoc;
}
}} // namespace indexlib::document
