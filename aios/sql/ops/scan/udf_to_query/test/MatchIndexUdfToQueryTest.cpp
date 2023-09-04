#include "sql/ops/scan/udf_to_query/MatchIndexUdfToQuery.h"

#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>
#include <string>
#include <vector>

#include "autil/legacy/RapidJsonCommon.h"
#include "ha3/common/Query.h"
#include "ha3/common/Term.h"
#include "ha3/common/TermQuery.h"
#include "ha3/turing/common/ModelConfig.h"
#include "navi/log/NaviLoggerProvider.h"
#include "sql/ops/scan/test/ScanConditionTestUtil.h"
#include "sql/ops/scan/udf_to_query/UdfToQuery.h"
#include "sql/ops/test/OpTestBase.h"
#include "unittest/unittest.h"

using namespace std;
using namespace suez::turing;

using namespace isearch::common;
using namespace isearch::turing;

namespace sql {
class MatchIndexUdfToQueryTest : public TESTBASE {
public:
    MatchIndexUdfToQueryTest();
    ~MatchIndexUdfToQueryTest();

public:
    void setUp();
    void tearDown();

private:
    MatchIndexUdfToQuery _matchIndexUdfToQuery;
    ScanConditionTestUtil _scanConditionTestUtil;
    std::shared_ptr<UdfToQueryParam> _param;
};

MatchIndexUdfToQueryTest::MatchIndexUdfToQueryTest() {}

MatchIndexUdfToQueryTest::~MatchIndexUdfToQueryTest() {}

void MatchIndexUdfToQueryTest::setUp() {
    _scanConditionTestUtil.init(GET_TEMPLATE_DATA_PATH(), GET_TEST_DATA_PATH());
    _param = _scanConditionTestUtil.createUdfToQueryParam();
}

void MatchIndexUdfToQueryTest::tearDown() {}

TEST_F(MatchIndexUdfToQueryTest, test1Param) {
    // 1 param
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"MATCHINDEX\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_matchIndexUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ("AndQuery", query->getQueryName());
    ASSERT_EQ(2, query->getChildQuery()->size());
    ASSERT_EQ("index_2",
              ((TermQuery *)((*query->getChildQuery())[0].get()))->getTerm().getIndexName());
}

TEST_F(MatchIndexUdfToQueryTest, test2Param_NameIndex) {
    // 2 param, index:name needn't token
    autil::SimpleDocument simpleDoc;
    string condStr
        = "{\"op\":\"MATCHINDEX\", \"params\":[\"name\", \"aa bb stop\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_matchIndexUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ("TermQuery", query->getQueryName());
    ASSERT_EQ("name", ((TermQuery *)(query.get()))->getTerm().getIndexName());
}

TEST_F(MatchIndexUdfToQueryTest, test3Param_noToken) {
    // 3 param, no token
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"MATCHINDEX\", \"params\":[\"index_2\", \"aa bb stop\", "
                     "\"tokenize_query:false\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_matchIndexUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ("TermQuery", query->getQueryName());
    ASSERT_EQ("index_2", ((TermQuery *)(query.get()))->getTerm().getIndexName());
}

TEST_F(MatchIndexUdfToQueryTest, test3Param_noToken2) {
    // 3 param, no token
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"MATCHINDEX\", \"params\":[\"index_2\", \"aa bb stop\", "
                     "\"remove_stopwords:false\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_matchIndexUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ(3, query->getChildQuery()->size());
}

TEST_F(MatchIndexUdfToQueryTest, testConditionIsNotObject) {
    // condition is not object
    autil::SimpleDocument simpleDoc;
    string condStr = "[]";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_matchIndexUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query == NULL);
}

TEST_F(MatchIndexUdfToQueryTest, testConditionOpNameError) {
    // condition op name error
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op1\":\"MATCHINDEX\", \"params\":[\"name\", \"1\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_matchIndexUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query == NULL);
}

TEST_F(MatchIndexUdfToQueryTest, testConditionParamNameError) {
    // condition param name error
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"MATCHINDEX\", \"params11\":[\"name\", \"1\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_matchIndexUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query == NULL);
}

TEST_F(MatchIndexUdfToQueryTest, testOpValueError) {
    // operator value error
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"!=\", \"params\":[\"$name\", \"1\"]}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_matchIndexUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query == NULL);
}

TEST_F(MatchIndexUdfToQueryTest, testParamSizeNotEqual0) {
    // params size not equal 0
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"MATCHINDEX\", \"params\":[]}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_matchIndexUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query == NULL);
}

TEST_F(MatchIndexUdfToQueryTest, testHaveColumnPrefix) {
    // have column prefix
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"MATCHINDEX\", \"params\":[\"$index_2\", \"aa bb stop\", "
                     "\"remove_stopwords:false\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_matchIndexUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query != NULL);
    ASSERT_EQ(3, query->getChildQuery()->size());
}

TEST_F(MatchIndexUdfToQueryTest, testNullQuery) {
    navi::NaviLoggerProvider provider("WARN");
    autil::SimpleDocument simpleDoc;
    string condStr
        = R"json({"op":"MATCHINDEX", "params":["index_2", "\"\"\"\"", "global_analyzer:fake_analyzer"], "type":"UDF"})json";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_matchIndexUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query == nullptr);
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "validate query failed: [\"\"\"\"]", traces);
}

} // namespace sql
