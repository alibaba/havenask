#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/TermQuery.h>
#include <autil/DataBuffer.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);

class TermQueryTest : public TESTBASE {
public:
    TermQueryTest();
    ~TermQueryTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, TermQueryTest);


TermQueryTest::TermQueryTest() { 
}

TermQueryTest::~TermQueryTest() { 
}

void TermQueryTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void TermQueryTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(TermQueryTest, testSerializeAndDeserialize) {
    RequiredFields requiredFields;
    Query *query = new TermQuery("abc", "default", requiredFields, "");
    ASSERT_EQ(MDL_TERM, query->getMatchDataLevel());
    ASSERT_EQ(string(""), query->getQueryLabel());
    query->setMatchDataLevel(MDL_TERM);
    query->setQueryLabel("TermLabel");
    ASSERT_EQ(string("TermLabel"), query->getQueryLabel());

    autil::DataBuffer dataBuffer;
    dataBuffer.write(query);

    Query *query2 = NULL;
    dataBuffer.read(query2);

    ASSERT_TRUE(*query2 == *query);


    ASSERT_EQ(query2->getMatchDataLevel(), query->getMatchDataLevel());
    ASSERT_EQ(query2->getQueryLabel(), query->getQueryLabel());
    ASSERT_EQ(string("TermLabel"), query2->getQueryLabel());
    ASSERT_EQ(MDL_TERM, query2->getMatchDataLevel());

    DELETE_AND_SET_NULL(query);
    DELETE_AND_SET_NULL(query2);
}

TEST_F(TermQueryTest, testConstructor) {
    RequiredFields requiredFields;
    {
        Term term("abc", "default", requiredFields);
        TermQuery query(term, "");
        ASSERT_EQ(MDL_TERM, query.getMatchDataLevel());
        ASSERT_EQ(string(""), query.getQueryLabel());
    }
    {
        Term term("abc", "default", requiredFields);
        TermQuery query(term, "label");
        ASSERT_EQ(MDL_TERM, query.getMatchDataLevel());
        ASSERT_EQ(string("label"), query.getQueryLabel());
    }
    {
        TermQuery query("abc", "default", requiredFields, "");
        ASSERT_EQ(MDL_TERM, query.getMatchDataLevel());
        ASSERT_EQ(string(""), query.getQueryLabel());
    }
    {
        TermQuery query("abc", "default", requiredFields, "label");
        ASSERT_EQ(MDL_TERM, query.getMatchDataLevel());
        ASSERT_EQ(string("label"), query.getQueryLabel());
    }
    {
        build_service::analyzer::Token token;
        TermQuery query(token, "default", requiredFields, "");
        ASSERT_EQ(MDL_TERM, query.getMatchDataLevel());
        ASSERT_EQ(string(""), query.getQueryLabel());
    }
    {
        build_service::analyzer::Token token;
        TermQuery query(token, "default", requiredFields, "label");
        ASSERT_EQ(MDL_TERM, query.getMatchDataLevel());
        ASSERT_EQ(string("label"), query.getQueryLabel());
    }
}

TEST_F(TermQueryTest, testSetQueryLabelWithDefaultLevel) {
    RequiredFields requiredFields;
    TermQuery query("abc", "default", requiredFields, "");
    query.setQueryLabelWithDefaultLevel("termLabel");
    ASSERT_EQ(MDL_TERM, query.getMatchDataLevel());
    ASSERT_EQ("termLabel", query.getQueryLabel());
}

END_HA3_NAMESPACE(common);

