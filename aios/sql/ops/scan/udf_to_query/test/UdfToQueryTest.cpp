#include "sql/ops/scan/udf_to_query/UdfToQuery.h"

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "autil/CommonMacros.h"
#include "build_service/analyzer/AnalyzerFactory.h"
#include "build_service/analyzer/Token.h"
#include "build_service/config/AgentGroupConfig.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryInfo.h"
#include "ha3/common/Term.h"
#include "ha3/common/TermQuery.h"
#include "ha3/search/PartitionInfoWrapper.h"
#include "ha3/turing/common/ModelConfig.h"
#include "matchdoc/ValueType.h"
#include "sql/ops/scan/Ha3ScanConditionVisitorParam.h"
#include "sql/ops/scan/test/ScanConditionTestUtil.h"
#include "unittest/unittest.h"

using namespace suez::turing;
using namespace build_service::analyzer;
using namespace build_service::config;
using namespace std;
using namespace testing;
using namespace matchdoc;
using namespace suez::turing;

using namespace isearch::search;
using namespace isearch::common;
using namespace isearch::turing;

namespace sql {

class UdfToQueryTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

private:
    ScanConditionTestUtil _scanConditionTestUtil;
};

void UdfToQueryTest::setUp() {
    _scanConditionTestUtil.init(GET_TEMPLATE_DATA_PATH(), GET_TEST_DATA_PATH());
}

void UdfToQueryTest::tearDown() {}

TEST_F(UdfToQueryTest, testTokenizeQuery) {
    Term term;
    term.setIndexName("index_2");
    term.setWord("a stop");
    QueryPtr query(new TermQuery(term, ""));
    unordered_set<string> noTokenIndexs;
    string analyzerName;
    unordered_map<string, string> indexAnalyzerName;
    Query *newQuery
        = UdfToQuery::tokenizeQuery(query.get(),
                                    _scanConditionTestUtil._queryInfo.getDefaultOperator(),
                                    noTokenIndexs,
                                    analyzerName,
                                    indexAnalyzerName,
                                    false,
                                    _scanConditionTestUtil._param.indexInfos,
                                    _scanConditionTestUtil._analyzerFactory.get());
    ASSERT_TRUE(query != NULL);
    ASSERT_TRUE(newQuery != NULL);
    ASSERT_EQ("AndQuery", newQuery->getQueryName());
    DELETE_AND_SET_NULL(newQuery);
}

TEST_F(UdfToQueryTest, testCleanStopWords) {
    { // is stopword
        build_service::analyzer::Token token("aa", 0);
        token.setIsStopWord(true);
        Term term(token, "name", RequiredFields());
        QueryPtr query(new TermQuery(term, ""));
        Query *newQuery = UdfToQuery::cleanStopWords(query.get());
        ASSERT_TRUE(query != NULL);
        ASSERT_TRUE(newQuery == NULL);
    }
    {
        build_service::analyzer::Token token("aa", 0);
        Term term(token, "name", RequiredFields());
        QueryPtr query(new TermQuery(term, ""));
        Query *newQuery = UdfToQuery::cleanStopWords(query.get());
        ASSERT_TRUE(query != NULL);
        ASSERT_TRUE(newQuery != NULL);
        DELETE_AND_SET_NULL(newQuery);
    }
}

TEST_F(UdfToQueryTest, testParseKvPairInfo) {
    {
        string parseStr = "remove_stopwords:false,tokenize_query:false,default_op:and,no_token_"
                          "indexes:a;b;c,global_analyzer:xxx,other:other1,specific_index_analyzer:"
                          "default#extend_analyzer;index2#aliws";
        string globalAnalyzer;
        string defaultOpStr;
        unordered_set<string> noTokenIndex;
        unordered_map<string, string> indexAnalyzerMap;
        bool tokenQuery = true;
        bool removeWords = true;
        UdfToQuery::parseKvPairInfo(parseStr,
                                    indexAnalyzerMap,
                                    globalAnalyzer,
                                    defaultOpStr,
                                    noTokenIndex,
                                    tokenQuery,
                                    removeWords);
        ASSERT_EQ("xxx", globalAnalyzer);
        ASSERT_EQ("and", defaultOpStr);
        ASSERT_FALSE(tokenQuery);
        ASSERT_FALSE(removeWords);
        ASSERT_THAT(noTokenIndex, UnorderedElementsAre("b", "c", "a"));
        ASSERT_EQ(2, indexAnalyzerMap.size());
        ASSERT_EQ("extend_analyzer", indexAnalyzerMap["default"]);
        ASSERT_EQ("aliws", indexAnalyzerMap["index2"]);
    }
}

} // namespace sql
