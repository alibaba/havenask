#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/test/Query2SummaryBaseTest.h>
#include <ha3/common/Term.h>
#include <ha3/common/PhraseQuery.h>
#include <ha3/common/Request.h>
#include <ha3/common/QueryClause.h>
#include <ha3/common/RequestConfig.h>

using namespace std;

USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(search);

class PhraseQuery2SummaryTest : public Query2SummaryBaseTest {
public:
    PhraseQuery2SummaryTest();
    ~PhraseQuery2SummaryTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};


HA3_LOG_SETUP(search, PhraseQuery2SummaryTest);


PhraseQuery2SummaryTest::PhraseQuery2SummaryTest() { 
}

PhraseQuery2SummaryTest::~PhraseQuery2SummaryTest() { 
}

void PhraseQuery2SummaryTest::setUp() { 
    HA3_LOG(DEBUG, "Begin PhraseQuery2SummaryTest");
    Query2SummaryBaseTest::setUp();
    Term term1("THIS", "phrase", ALL_FIELDBITS);
    Term term2("IS", "phrase", ALL_FIELDBITS);
    Term term3("A", "phrase", ALL_FIELDBITS);
    PhraseQuery *phraseQuery = new PhraseQuery();
    phraseQuery->addTerm(term1);
    phraseQuery->addTerm(term2);
    phraseQuery->addTerm(term3);

    Request request;
    RequestConfig *requestConfig = new RequestConfig();
    request.setRequestConfig(requestConfig);
    requestConfig->setHitCount(10);

    QueryClause *queryClause = new QueryClause();
    request.setQueryClause(queryClause);
    queryClause->setRootQuery(phraseQuery);

    processRequest(&request);
    HA3_LOG(DEBUG, "END PhraseQuery2SummaryTest");
}

void PhraseQuery2SummaryTest::tearDown() { 
    Query2SummaryBaseTest::tearDown();
}

TEST_F(PhraseQuery2SummaryTest, testHitsSize) { 
    HA3_LOG(DEBUG, "Begin");
    Hits* hits = _result->getHits();
    ASSERT_EQ((uint32_t)1, hits->size());
}

TEST_F(PhraseQuery2SummaryTest, testNumHits) {
    HA3_LOG(DEBUG, "Begin");
    Hits* hits = _result->getHits();
    ASSERT_EQ((uint32_t)1, hits->totalHits());
}

TEST_F(PhraseQuery2SummaryTest, testHits) {
    HA3_LOG(DEBUG, "Begin");
    Hits* hits = _result->getHits();
    ASSERT_EQ((docid_t)0, hits->getDocId(0));
    ASSERT_EQ((docid_t)INVALID_DOCID, hits->getDocId(1));
}

TEST_F(PhraseQuery2SummaryTest, testDocHits) {
    //ASSERT_EQ((uint32_t)3, hits->getFieldCount());

    Hits* hits = _result->getHits();
    const Hit *hit = hits->getHit(0);
    ASSERT_TRUE(hit);

    //ASSERT_EQ(string("title"), hits->getFieldName(0));
    ASSERT_EQ(string("this is a test title"), 
                         hit->getFieldValue(0));

    //ASSERT_EQ(string("body"), hits->getFieldName(1));
    ASSERT_EQ(string("guorijie likes feet washing, he offen go to "
                                "washing location in hangzhou"),
                         hit->getFieldValue(1));

    //ASSERT_EQ(string("description"), hits->getFieldName(2));
    ASSERT_EQ(string("alibaba company locates in hangzhou, there"
                                " are more than 10000 people in this company"),
                         hit->getFieldValue(2));
}

END_HA3_NAMESPACE(search);

