#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/FilterClause.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>

using namespace std;
using namespace suez::turing;

USE_HA3_NAMESPACE(queryparser);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(qrs);

BEGIN_HA3_NAMESPACE(common);

class FilterClauseTest : public TESTBASE {
public:
    FilterClauseTest();
    ~FilterClauseTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, FilterClauseTest);


FilterClauseTest::FilterClauseTest() { 
}

FilterClauseTest::~FilterClauseTest() { 
}

void FilterClauseTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void FilterClauseTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(FilterClauseTest, testSerialize) { 
    HA3_LOG(DEBUG, "Begin Test!");
    SyntaxExpr* syntaxExpr = new AtomicSyntaxExpr("a",
            vt_int32, ATTRIBUTE_NAME);
    FilterClause filterClause;
    filterClause.setRootSyntaxExpr(syntaxExpr);
    filterClause.setOriginalString("a");
    
    //serialize and deserialize
    FilterClause filterClause2;
    autil::DataBuffer buffer;
    filterClause.serialize(buffer);
    filterClause2.deserialize(buffer);

    const SyntaxExpr *syntaxExpr2 = filterClause2.getRootSyntaxExpr();
    ASSERT_EQ(filterClause.getOriginalString(), 
                         filterClause2.getOriginalString());

    ASSERT_TRUE(*syntaxExpr == syntaxExpr2);
}

TEST_F(FilterClauseTest, testToString) {
    string requestStr = "filter=user_id=5 AND price>20";
    RequestPtr requestPtr = RequestCreator::prepareRequest(requestStr);
    ASSERT_TRUE(requestPtr);
    FilterClause* filterClause 
        = requestPtr->getFilterClause();
    ASSERT_EQ(string("((user_id=5) AND (price>20))"), 
                         filterClause->toString());
}

END_HA3_NAMESPACE(common);

