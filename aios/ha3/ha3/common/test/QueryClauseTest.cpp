#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Query.h>
#include <ha3/common/AndQuery.h>
#include <ha3/common/QueryClause.h>
#include <ha3/common/test/QueryConstructor.h>
#include <ha3/common/OrQuery.h>
#include <ha3/common/NumberQuery.h>
#include <ha3/common/PhraseQuery.h>
#include <ha3/common/AndQuery.h>
#include <ha3/common/OrQuery.h>
#include <ha3/common/RankQuery.h>
#include <ha3/common/AndNotQuery.h>
#include <ha3/common/TermQuery.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);

class QueryClauseTest : public TESTBASE {
public:
    QueryClauseTest();
    ~QueryClauseTest();
public:
    void setUp();
    void tearDown();
protected:
    Query* prepareMultiQuery();
protected:
    autil::DataBuffer _buffer;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, QueryClauseTest);


QueryClauseTest::QueryClauseTest() { 
}

QueryClauseTest::~QueryClauseTest() { 
}

void QueryClauseTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _buffer.clear();
}

void QueryClauseTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(QueryClauseTest, testBinaryEmptyQuerySerialize)
{
    QueryClause queryClause;
    queryClause.setOriginalString("null");

    queryClause.serialize(_buffer);

    QueryClause queryClause2;
    queryClause2.deserialize(_buffer);

    ASSERT_EQ(string("null"), queryClause.getOriginalString());
    ASSERT_TRUE(!queryClause.getRootQuery());
}

TEST_F(QueryClauseTest, testBinaryMultiSerialize)
{
    HA3_LOG(DEBUG, "Begin Test!");

    string originalString = "test";
    Query *query = prepareMultiQuery();
    QueryClause queryClause;
    queryClause.setRootQuery(query);
    queryClause.setOriginalString(originalString);

    _buffer.write(queryClause);

    QueryClause queryClause2;
    _buffer.read(queryClause2);

    ASSERT_EQ(originalString, queryClause.getOriginalString());

    const Query *resultQuery = queryClause2.getRootQuery();
    ASSERT_TRUE(resultQuery);
    ASSERT_EQ(*((AndQuery *)query), *((AndQuery *)resultQuery)); 
}

Query* QueryClauseTest::prepareMultiQuery()
{
    string originalString = "query="
                            "(phrase:one^2 OR phrase:(1,2])"
                            " AND "
                            "((phrase:two ANDNOT phrase:three) RANK phrase:\"four five\")";

    AndQuery *andQuery = new AndQuery("");

    OrQuery *orQuery = new OrQuery("");
    QueryPtr orQueryPtr(orQuery);
    RequiredFields requiredFields;
    Term term("one", "phrase", requiredFields);
    TermQuery *termQuery = new TermQuery(term, "");
    QueryPtr termQueryPtr(termQuery);
    orQuery->addQuery(termQueryPtr);
    NumberTerm numberTerm(1, false, 2, true, 
                          "phrase", requiredFields);
    NumberQuery *numberQuery = new NumberQuery(numberTerm, "");
    QueryPtr numberQueryPtr(numberQuery);
    orQuery->addQuery(numberQueryPtr);

    RankQuery *rankQuery = new RankQuery("");
    QueryPtr rankQueryPtr(rankQuery);
    AndNotQuery *andNotQuery = new AndNotQuery("");
    QueryPtr andNotQueryPtr(andNotQuery);
    QueryConstructor::prepare(andNotQuery, "phrase", "two", "three");
    rankQuery->addQuery(andNotQueryPtr);

    TermPtr term4(new Term("four", "phrase", requiredFields));
    TermPtr term5(new Term("five", "phrase", requiredFields));
    PhraseQuery *phraseQuery = new PhraseQuery("");
    QueryPtr phraseQueryPtr(phraseQuery);
    phraseQuery->addTerm(term4);
    phraseQuery->addTerm(term5);
    rankQuery->addQuery(phraseQueryPtr);
    
    andQuery->addQuery(orQueryPtr);
    andQuery->addQuery(rankQueryPtr);
    return andQuery;
}

TEST_F(QueryClauseTest, testToString) { 
    HA3_LOG(DEBUG, "Begin Test!");
    {
        string query1 = "query=phrase(feature, type):aa AND bb";
        RequestPtr requestPtr1 = qrs::RequestCreator::prepareRequest(query1);
        ASSERT_TRUE(requestPtr1);
        QueryClause* queryClause1 = requestPtr1->getQueryClause();
        ASSERT_EQ(string("AndQuery:[TermQuery:[Term:[phrase|feature and type|aa|100|]], "
                         "TermQuery:[Term:[default0||bb|100|]], ]|"),
                  queryClause1->toString());
    }
    {
        string query = "query=phrase:aa";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("TermQuery:[Term:[phrase||aa|100|]]|"), queryClause->toString());
    }
    {
        string query = "query=phrase:aa|bb|cc";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query,
                "default0", true);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("MultiTermQuery:[Term:[phrase||aa|100|], Term:[phrase||bb|100|], Term:[phrase||cc|100|], ]|"), queryClause->toString());
    }
    {
        string query = "query=phrase:\"aa\"";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("PhraseQuery:[Term:[phrase||aa|100|], ]|"), queryClause->toString());
    }
    {
        string query = "query=phrase:[0,100]";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("NumberQuery:[NumberTerm: [phrase,[0, 100||[0,100]|100|]]|"), queryClause->toString());
    }
    {
        string query = "query=aa AND bb";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("AndQuery:[TermQuery:[Term:[default0||aa|100|]], TermQuery:[Term:[default0||bb|100|]], ]|"), queryClause->toString());
    }
    {
        string query = "query=aa OR bb";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("OrQuery:[TermQuery:[Term:[default0||aa|100|]], TermQuery:[Term:[default0||bb|100|]], ]|"), queryClause->toString());
    }
    {
        string query = "query=aa ANDNOT bb";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("AndNotQuery:[TermQuery:[Term:[default0||aa|100|]], TermQuery:[Term:[default0||bb|100|]], ]|"), queryClause->toString());
    }
    {
        string query = "query=aa RANK bb";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("RankQuery:[TermQuery:[Term:[default0||aa|100|]], TermQuery:[Term:[default0||bb|100|]], ]|"), queryClause->toString());
    }
}

TEST_F(QueryClauseTest, testToStringWithLabel) { 
    HA3_LOG(DEBUG, "Begin Test!");
    {
        string query1 = "query=(phrase(feature, type):aa AND bb)@label";
        RequestPtr requestPtr1 = qrs::RequestCreator::prepareRequest(query1);
        ASSERT_TRUE(requestPtr1);
        QueryClause* queryClause1 = requestPtr1->getQueryClause();
        ASSERT_EQ(string("AndQuery@label:[TermQuery:[Term:[phrase|feature and type|aa|100|]], "
                         "TermQuery:[Term:[default0||bb|100|]], ]|"),
                  queryClause1->toString());
    }
    {
        string query = "query=(phrase:aa)@label";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("TermQuery@label:[Term:[phrase||aa|100|]]|"), queryClause->toString());
    }
    {
        string query = "query=(phrase:aa|bb|cc)@label";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query,
                "default0", true);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("MultiTermQuery@label:[Term:[phrase||aa|100|], Term:[phrase||bb|100|], Term:[phrase||cc|100|], ]|"), queryClause->toString());
    }
    {
        string query = "query=(phrase:aa&bb&cc)@label";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query,
                "default0", true);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("MultiTermQuery@label:[Term:[phrase||aa|100|], Term:[phrase||bb|100|], Term:[phrase||cc|100|], ]|"), queryClause->toString());
    }
    {
        string query = "query=(phrase:aa||bb||cc)@label";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query,
                "default0", true);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("MultiTermQuery@label:[Term:[phrase||aa|100|], Term:[phrase||bb|100|], Term:[phrase||cc|100|], ]|"), queryClause->toString());
    }
    {
        string query = "query=(phrase:(aa||bb||cc)^2)@label";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query,
                "default0", true);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("MultiTermQuery@label:[Term:[phrase||aa|100|], Term:[phrase||bb|100|], Term:[phrase||cc|100|], ]|"), queryClause->toString());
    }
    {
        string query = "query=(phrase:aa|bb|cc)@label";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query,
                "default0", false);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("OrQuery@label:[OrQuery:[TermQuery:[Term:[phrase||aa|100|]],"
                         " TermQuery:[Term:[phrase||bb|100|]], ],"
                         " TermQuery:[Term:[phrase||cc|100|]], ]|"),
                  queryClause->toString());
    }
    {
        string query = "query=(phrase:aa&bb&cc)@label";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query,
                "default0", false);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("AndQuery@label:[AndQuery:[TermQuery:[Term:[phrase||aa|100|]],"
                         " TermQuery:[Term:[phrase||bb|100|]], ],"
                         " TermQuery:[Term:[phrase||cc|100|]], ]|"),
                  queryClause->toString());
    }
    {
        string query = "query=(phrase:(aa||bb||cc))@label";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query,
                "default0", false);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("OrQuery@label:[OrQuery:[TermQuery:[Term:[phrase||aa|100|]],"
                         " TermQuery:[Term:[phrase||bb|100|]], ],"
                         " TermQuery:[Term:[phrase||cc|100|]], ]|"),
                  queryClause->toString());
    }
    {
        string query = "query=(phrase:(aa||bb||cc)^2)@label";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query,
                "default0", false);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("OrQuery@label:[OrQuery:[TermQuery:[Term:[phrase||aa|100|]],"
                         " TermQuery:[Term:[phrase||bb|100|]], ],"
                         " TermQuery:[Term:[phrase||cc|100|]], ]|"),
                  queryClause->toString());
    }
    {
        string query = "query=(phrase:\"aa\")@label";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("PhraseQuery@label:[Term:[phrase||aa|100|], ]|"), queryClause->toString());
    }
    {
        string query = "query=(phrase:[0,100])@label";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("NumberQuery@label:[NumberTerm: [phrase,[0, 100||[0,100]|100|]]|"), queryClause->toString());
    }
    {
        string query = "query=(aa AND bb)@label";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("AndQuery@label:[TermQuery:[Term:[default0||aa|100|]], TermQuery:[Term:[default0||bb|100|]], ]|"), queryClause->toString());
    }
    {
        string query = "query=(aa OR bb)@label";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("OrQuery@label:[TermQuery:[Term:[default0||aa|100|]], TermQuery:[Term:[default0||bb|100|]], ]|"), queryClause->toString());
    }
    {
        string query = "query=(aa ANDNOT bb)@label";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("AndNotQuery@label:[TermQuery:[Term:[default0||aa|100|]], TermQuery:[Term:[default0||bb|100|]], ]|"), queryClause->toString());
    }
    {
        string query = "query=(aa RANK bb)@label";
        RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(query);
        ASSERT_TRUE(requestPtr);
        QueryClause* queryClause = requestPtr->getQueryClause();
        ASSERT_EQ(string("RankQuery@label:[TermQuery:[Term:[default0||aa|100|]], TermQuery:[Term:[default0||bb|100|]], ]|"), queryClause->toString());
    }
}

END_HA3_NAMESPACE(common);

