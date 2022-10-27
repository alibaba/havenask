#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Hits.h>
#include <ha3/search/test/Query2SummaryBaseTest.h>
#include <ha3/common/Term.h>
#include <ha3/common/TermQuery.h>
#include <string>
#include <memory>
#include <build_service/plugin/PlugInManager.h>

using namespace std;

BEGIN_HA3_NAMESPACE(search);

class TermQuery2SummaryTest : public Query2SummaryBaseTest {
public:
    TermQuery2SummaryTest();
    ~TermQuery2SummaryTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

USE_HA3_NAMESPACE(common);
HA3_LOG_SETUP(search, TermQuery2SummaryTest);


TermQuery2SummaryTest::TermQuery2SummaryTest() { 
}

TermQuery2SummaryTest::~TermQuery2SummaryTest() { 

}

void TermQuery2SummaryTest::setUp() { 
    HA3_LOG(DEBUG, "Begin");
    Query2SummaryBaseTest::setUp();
    Term term("ALIBABA", "phrase", ALL_FIELDBITS);
    TermQuery *termQuery = new TermQuery(term);

    Request request;
    RequestConfig *requestConfig = new RequestConfig();
    requestConfig->setHitCount(10);
    request.setRequestConfig(requestConfig);

    QueryClause* queryClause = new QueryClause(termQuery);
    request.setQueryClause(queryClause);
    
    processRequest(&request);
    HA3_LOG(DEBUG, "End");
}

void TermQuery2SummaryTest::tearDown() { 
    Query2SummaryBaseTest::tearDown();
}

TEST_F(TermQuery2SummaryTest, testHitsSize) { 
    HA3_LOG(DEBUG, "Begin");
    Hits* hits = _result->getHits();
    ASSERT_EQ((uint32_t)2, hits->size());
}

TEST_F(TermQuery2SummaryTest, testNumHits) {
    HA3_LOG(DEBUG, "Begin");
    Hits* hits = _result->getHits();
    ASSERT_EQ((uint32_t)2, hits->totalHits());
}

TEST_F(TermQuery2SummaryTest, testHits) {
    HA3_LOG(DEBUG, "Begin");
    Hits* hits = _result->getHits();
    ASSERT_EQ((docid_t)0, hits->getDocId(0));
    ASSERT_EQ((docid_t)1, hits->getDocId(1));
}

TEST_F(TermQuery2SummaryTest, testDocHits) {
    Hits* hits = _result->getHits();
    const Hit *hit = hits->getHit(0);

    ASSERT_TRUE(hit);
    //ASSERT_EQ((uint32_t)3, hits->getFieldCount());

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

TEST_F(TermQuery2SummaryTest, testDocHits2) {
    Hits* hits = _result->getHits();
    const Hit *hit = hits->getHit(1);
    ASSERT_TRUE(hit);
    //ASSERT_EQ((uint32_t)3, hits->getFieldCount());

    //ASSERT_EQ(string("title"), hits->getFieldName(0));
    ASSERT_EQ(string("zhangli arrived in  beijing"), 
                         hit->getFieldValue(0));

    //ASSERT_EQ(string("body"), hits->getFieldName(1));
    ASSERT_EQ(string("zhangli arrived in beijing by airplane at last"
                                " weekend, he will invite us to have a deal"),
                         hit->getFieldValue(1));

    //ASSERT_EQ(string("description"), hits->getFieldName(2));
    ASSERT_EQ(string("zhangli's job is to develop isearch4.0, "
                                "which is a big project of alibaba company"),
                         hit->getFieldValue(2));
}    

END_HA3_NAMESPACE(search);

