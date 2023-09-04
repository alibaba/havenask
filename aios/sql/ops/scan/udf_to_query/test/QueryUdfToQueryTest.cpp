#include "ha3/common/Query.h"

#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>
#include <string>
#include <vector>

#include "autil/legacy/RapidJsonCommon.h"
#include "ha3/common/Term.h"
#include "ha3/common/TermQuery.h"
#include "ha3/turing/common/ModelConfig.h"
#include "navi/log/NaviLoggerProvider.h"
#include "sql/ops/scan/test/ScanConditionTestUtil.h"
#include "sql/ops/scan/udf_to_query/QueryUdfToQuery.h"
#include "sql/ops/test/OpTestBase.h"
#include "suez/turing/expression/cava/common/CavaConfig.h"
#include "unittest/unittest.h"

namespace sql {
class UdfToQueryParam;
} // namespace sql

using namespace std;
using namespace suez::turing;

using namespace isearch::common;
using namespace isearch::turing;

namespace sql {
class QueryUdfToQueryTest : public TESTBASE {
public:
    QueryUdfToQueryTest();
    ~QueryUdfToQueryTest();

public:
    void setUp();
    void tearDown();

private:
    QueryUdfToQuery _queryUdfToQuery;
    ScanConditionTestUtil _scanConditionTestUtil;
    std::shared_ptr<UdfToQueryParam> _param;
};

QueryUdfToQueryTest::QueryUdfToQueryTest() {}

QueryUdfToQueryTest::~QueryUdfToQueryTest() {}

void QueryUdfToQueryTest::setUp() {
    _scanConditionTestUtil.init(GET_TEMPLATE_DATA_PATH(), GET_TEST_DATA_PATH());
    _param = _scanConditionTestUtil.createUdfToQueryParam();
}

void QueryUdfToQueryTest::tearDown() {}

TEST_F(QueryUdfToQueryTest, test1Param_index2) {
    // 1 param , use default index_2
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_queryUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ("AndQuery", query->getQueryName());
    ASSERT_EQ(2, query->getChildQuery()->size());
    ASSERT_EQ("index_2",
              ((TermQuery *)((*query->getChildQuery())[0].get()))->getTerm().getIndexName());
}

TEST_F(QueryUdfToQueryTest, test2param_nameIndex) {
    // 2 param, index:name needn't token
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"QUERY\", \"params\":[\"name\", \"aa bb\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_queryUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ("AndQuery", query->getQueryName());
    ASSERT_EQ(2, query->getChildQuery()->size());
    ASSERT_EQ("name",
              ((TermQuery *)((*query->getChildQuery())[0].get()))->getTerm().getIndexName());
}

TEST_F(QueryUdfToQueryTest, test2Param_or) {
    // 2 param, use or
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"QUERY\", \"params\":[\"name\", \"aa bb\", "
                     "\"default_op:or\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_queryUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ("OrQuery", query->getQueryName());
    ASSERT_EQ(2, query->getChildQuery()->size());
    ASSERT_EQ("name",
              ((TermQuery *)((*query->getChildQuery())[0].get()))->getTerm().getIndexName());
}

TEST_F(QueryUdfToQueryTest, test3Param_noToken) {
    // 3 param, no token
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"QUERY\", \"params\":[ \"index_2\", \"aa bb stop\", "
                     "\"tokenize_query:false\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_queryUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ("AndQuery", query->getQueryName());
    ASSERT_EQ(2, query->getChildQuery()->size());
    ASSERT_EQ("index_2",
              ((TermQuery *)((*query->getChildQuery())[0].get()))->getTerm().getIndexName());
}

TEST_F(QueryUdfToQueryTest, test3Param_noToken2) {
    // 3 param, no token
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"QUERY\", \"params\":[\"index_2\", \"aa bb stop\", "
                     "\"remove_stopwords:false\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_queryUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ(2, query->getChildQuery()->size());
}

TEST_F(QueryUdfToQueryTest, testColumnPrefix) {
    // 4 have column prefix
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"QUERY\", \"params\":[\"$index_2\", \"aa bb stop\", "
                     "\"remove_stopwords:false\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_queryUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ(2, query->getChildQuery()->size());
}

TEST_F(QueryUdfToQueryTest, testCondtionNotObject) {
    // condition is not object
    autil::SimpleDocument simpleDoc;
    string condStr = "[]";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_queryUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query == NULL);
}

TEST_F(QueryUdfToQueryTest, testConditionOpNameError) {
    // condition op name error
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op1\":\"QUERY\", \"params\":[\"$name\", 1],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_queryUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query == NULL);
}

TEST_F(QueryUdfToQueryTest, testConditionParamNameError) {
    // condition param name error
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"QUERY\", \"params11\":[\"$name\", 1],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_queryUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query == NULL);
}

TEST_F(QueryUdfToQueryTest, testOpValueError) {
    // operator value error
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"!=\", \"params\":[\"$name\", 1]}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_queryUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query == NULL);
}

TEST_F(QueryUdfToQueryTest, testParamSizeNotEqual0) {
    // params size not equal 0
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"QUERY\", \"params\":[]}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_queryUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query == NULL);
}

TEST_F(QueryUdfToQueryTest, testNullQuery) {
    navi::NaviLoggerProvider provider("WARN");
    autil::SimpleDocument simpleDoc;
    string condStr
        = R"json({"op":"QUERY", "params":["index_2", "\"\"\"\"", "global_analyzer:fake_analyzer"], "type":"UDF"})json";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_queryUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query == nullptr);
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "validate query failed, error msg [Query is null]", traces);
    CHECK_TRACE_COUNT(1, "validate query failed: [\"\"\"\"]", traces);
}

} // namespace sql
