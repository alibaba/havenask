#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/OptimizerClause.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>
#include <ha3/common/Request.h>

using namespace std;
BEGIN_HA3_NAMESPACE(common);

class OptimizerClauseTest : public TESTBASE {
public:
    OptimizerClauseTest();
    ~OptimizerClauseTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, OptimizerClauseTest);


OptimizerClauseTest::OptimizerClauseTest() { 
}

OptimizerClauseTest::~OptimizerClauseTest() { 
}

void OptimizerClauseTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void OptimizerClauseTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(OptimizerClauseTest, testToString) { 
    HA3_LOG(DEBUG, "Begin Test!");
    {
        string query1 = "config=cluster:cluster1,rank_size:10&&query=phrase:with"
                        "&&optimizer=name:AuxChain,option:select#ALL|pos#AFTER|aux_name#weight";
        RequestPtr requestPtr1 = qrs::RequestCreator::prepareRequest(query1);
        ASSERT_TRUE(requestPtr1);
        OptimizerClause* optimizerClause1 = requestPtr1->getOptimizerClause();
        ASSERT_EQ(string("AuxChain:select#ALL|pos#AFTER|aux_name#weight;"), 
                             optimizerClause1->toString());
    }

    {
        string query1 = "config=cluster:cluster1,rank_size:10&&query=phrase:with"
                        "&&optimizer=name:ChainOptimizer2,option:select#ALL|pos#AFTER;"
                        "name:ChainOptimizer1,option:select#ALL|pos#AFTER|aux_name#weight";
        RequestPtr requestPtr1 = qrs::RequestCreator::prepareRequest(query1);
        ASSERT_TRUE(requestPtr1);
        OptimizerClause* optimizerClause1 = requestPtr1->getOptimizerClause();
        ASSERT_EQ(string("ChainOptimizer2:select#ALL|pos#AFTER;"
                        "ChainOptimizer1:select#ALL|pos#AFTER|aux_name#weight;"), 
                             optimizerClause1->toString());
    }
}
END_HA3_NAMESPACE(common);

