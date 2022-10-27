#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/FinalSortClause.h>
#include <ha3/test/test.h>
#include <ha3/common/Request.h>
#include <ha3/queryparser/RequestParser.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>

BEGIN_HA3_NAMESPACE(common);

class FinalSortClauseTest : public TESTBASE {
public:
    FinalSortClauseTest();
    ~FinalSortClauseTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, FinalSortClauseTest);

USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(queryparser);
using namespace std;


FinalSortClauseTest::FinalSortClauseTest() { 
}

FinalSortClauseTest::~FinalSortClauseTest() { 
}

void FinalSortClauseTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void FinalSortClauseTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(FinalSortClauseTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    RequestPtr requestPtr(new Request);
    RequestParser requestParser;
    {
        string oriStr = "sort_name:DEFAULT;"
                        "sort_param:key1#value1,key2#value2";
        requestPtr->setOriginalString("final_sort=" + oriStr);
        FinalSortClause *finalSortClause = requestPtr->getFinalSortClause();
        ASSERT_TRUE(finalSortClause);
        ASSERT_TRUE(requestParser.parseFinalSortClause(requestPtr));
        ASSERT_EQ(string("DEFAULT"), 
                             finalSortClause->getSortName());
        ASSERT_EQ(true, 
                             finalSortClause->useDefaultSort());
        map<string, string> paramMap = finalSortClause->getParams();
        ASSERT_EQ(string("value1"), paramMap["key1"]);
        ASSERT_EQ(string("value2"), paramMap["key2"]);
        ASSERT_EQ(oriStr, finalSortClause->getOriginalString());
    }
}

TEST_F(FinalSortClauseTest, testSerializeAndDeserialize) {
    RequestPtr requestPtr(new Request);
    RequestParser requestParser;

    string oriStr = "sort_name:UserDefinedSort;"
                    "sort_param:key1#value1,key2#value2";
    requestPtr->setOriginalString("final_sort=" + oriStr);
    FinalSortClause *finalSortClause = requestPtr->getFinalSortClause();
    ASSERT_TRUE(finalSortClause);
    ASSERT_TRUE(requestParser.parseFinalSortClause(requestPtr));
    FinalSortClause deserializedFinalSortCaluse;

    autil::DataBuffer dataBuffer;
    finalSortClause->serialize(dataBuffer);
    deserializedFinalSortCaluse.deserialize(dataBuffer);

    ASSERT_EQ(string("UserDefinedSort"), 
                         deserializedFinalSortCaluse.getSortName());
    ASSERT_EQ(false, 
                         deserializedFinalSortCaluse.useDefaultSort());
    map<string, string> paramMap = deserializedFinalSortCaluse.getParams();
    ASSERT_EQ(string("value1"), paramMap["key1"]);
    ASSERT_EQ(string("value2"), paramMap["key2"]);
    ASSERT_EQ(oriStr, 
                         deserializedFinalSortCaluse.getOriginalString());
}

TEST_F(FinalSortClauseTest, testToString) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string query1 = "final_sort=sort_name:DEFAULT;sort_param:sort_key1#-RANK|+id,sort_key2#sort_value";
    RequestPtr requestPtr1 = qrs::RequestCreator::prepareRequest(query1);
    ASSERT_TRUE(requestPtr1);
    FinalSortClause* finalSortClause1 = requestPtr1->getFinalSortClause();
    ASSERT_EQ(string("DEFAULT[sort_key1:-RANK|+id,sort_key2:sort_value,]"),
                         finalSortClause1->toString());
}

END_HA3_NAMESPACE(common);

