#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/test/test.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>
#include <ha3/common/SortClause.h>

using namespace std;
using namespace suez::turing;

USE_HA3_NAMESPACE(queryparser);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(qrs);

BEGIN_HA3_NAMESPACE(common);

class SortClauseTest : public TESTBASE {
public:
    SortClauseTest();
    ~SortClauseTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, SortClauseTest);


SortClauseTest::SortClauseTest() { 
}

SortClauseTest::~SortClauseTest() { 
}

void SortClauseTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void SortClauseTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(SortClauseTest, testSerializeAndDeserialize) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string originalString = "sort=type;+userid";
    SortClause sortClause;
    sortClause.setOriginalString(originalString);

    SortDescription *sortDescription1 = new SortDescription("type");
    SyntaxExpr* sortExpr1 = new AtomicSyntaxExpr("type",
            vt_int32, ATTRIBUTE_NAME);
    sortDescription1->setRootSyntaxExpr(sortExpr1);
    sortDescription1->setExpressionType(SortDescription::RS_NORMAL);
    sortClause.addSortDescription(sortDescription1);
    HA3_LOG(DEBUG, "original string befor jsonize: %s",
        sortDescription1->getOriginalString().c_str());

    SortDescription *sortDescription2 = new SortDescription("userid");
    SyntaxExpr* sortExpr2 = new AtomicSyntaxExpr("userid",
            vt_int32, ATTRIBUTE_NAME);    
    sortDescription2->setRootSyntaxExpr(sortExpr2);
    sortDescription2->setExpressionType(SortDescription::RS_NORMAL);
    sortDescription2->setSortAscendFlag(true);
    sortClause.addSortDescription(sortDescription2);

    SortClause sortClause2;
    autil::DataBuffer buffer;
    buffer.write(sortClause);
    buffer.read(sortClause2);

    HA3_LOG(DEBUG, "sort clause original string after jsonize: %s",
        sortClause2.getOriginalString().c_str());

    ASSERT_EQ(originalString, sortClause2.getOriginalString());
    const vector<SortDescription*>& sortDescriptions = 
        sortClause2.getSortDescriptions();
    ASSERT_EQ((size_t)2, sortDescriptions.size());

    ASSERT_EQ(string("type"), sortDescriptions[0]->getOriginalString());
    ASSERT_EQ(false, sortDescriptions[0]->getSortAscendFlag());
    ASSERT_TRUE(*sortExpr1 == sortDescriptions[0]->getRootSyntaxExpr());

    ASSERT_EQ(string("userid"), sortDescriptions[1]->getOriginalString());
    ASSERT_EQ(true, sortDescriptions[1]->getSortAscendFlag());
    ASSERT_TRUE(*sortExpr2 == sortDescriptions[1]->getRootSyntaxExpr());
}

TEST_F(SortClauseTest, testToString) {
    string requestStr = "sort=+RANK;-( a - b )";
    RequestPtr requestPtr = RequestCreator::prepareRequest(requestStr);
    ASSERT_TRUE(requestPtr);
    SortClause* sortClause 
        = requestPtr->getSortClause();
    ASSERT_EQ(string("+(RANK);-((a-b));"), sortClause->toString());
}

END_HA3_NAMESPACE(common);

