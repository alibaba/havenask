#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/QueryExecutorRestrictor.h>
#include <ha3/search/test/FakeTimeoutTerminator.h>

using namespace std;

BEGIN_HA3_NAMESPACE(search);

class QueryExecutorRestrictorTest : public TESTBASE {
public:
    QueryExecutorRestrictorTest();
    ~QueryExecutorRestrictorTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, QueryExecutorRestrictorTest);


QueryExecutorRestrictorTest::QueryExecutorRestrictorTest() { 
}

QueryExecutorRestrictorTest::~QueryExecutorRestrictorTest() { 
}

void QueryExecutorRestrictorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void QueryExecutorRestrictorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(QueryExecutorRestrictorTest, testCheckLayerMeta) { 
    QueryExecutorRestrictor restrictor;
    autil::mem_pool::Pool pool(1024);
    LayerMeta layerMeta(&pool);
    layerMeta.push_back(DocIdRangeMeta(0, 10));
    layerMeta.push_back(DocIdRangeMeta(20, 30));
    restrictor.setLayerMeta(&layerMeta);
    
    ASSERT_EQ(0, restrictor.meetRestrict(0));
    ASSERT_EQ(9, restrictor.meetRestrict(9));
    ASSERT_EQ(10, restrictor.meetRestrict(10));
    ASSERT_EQ(20, restrictor.meetRestrict(11));
    ASSERT_EQ(20, restrictor.meetRestrict(20));
    ASSERT_EQ(30, restrictor.meetRestrict(30));
    ASSERT_EQ(END_DOCID, restrictor.meetRestrict(31));
}

TEST_F(QueryExecutorRestrictorTest, testCheckTimeout) { 
    QueryExecutorRestrictor restrictor;
    int32_t checkStep = common::TimeoutTerminator::RESTRICTOR_CHECK_MASK + 1;
    FakeTimeoutTerminator timeoutTerminator(2);
    restrictor.setTimeoutTerminator(&timeoutTerminator);
    docid_t docid = 0;
    for (int32_t i = 0; i < checkStep; ++i) {
        ASSERT_EQ(docid, restrictor.meetRestrict(docid));
    }
    ASSERT_EQ(END_DOCID, restrictor.meetRestrict(docid));
}

END_HA3_NAMESPACE(search);

