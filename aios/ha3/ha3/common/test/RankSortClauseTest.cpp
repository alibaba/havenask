#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/RankSortClause.h>
#include <autil/StringUtil.h>
#include <ha3/common/Request.h>
#include <ha3/common/SortClause.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>

using namespace autil;
BEGIN_HA3_NAMESPACE(common);

class RankSortClauseTest : public TESTBASE {
public:
    RankSortClauseTest();
    ~RankSortClauseTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, RankSortClauseTest);


RankSortClauseTest::RankSortClauseTest() { 
}

RankSortClauseTest::~RankSortClauseTest() { 
}

void RankSortClauseTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void RankSortClauseTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(RankSortClauseTest, testToString) { 
    HA3_LOG(DEBUG, "Begin Test!");
    std::string requestStr = "rank_sort=sort:-sell_count#-RANK,percent:40;sort:-uvsum#-RANK,percent:30;sort:+price#-RANK,percent:20;sort:-RANK,percent:10";
    RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(requestStr);
    ASSERT_TRUE(requestPtr);
    RankSortClause *rankSortClause 
        = requestPtr->getRankSortClause();
    ASSERT_EQ(std::string("-(sell_count)#-(RANK)#,percent:40;-(uvsum)#-(RANK)#,percent:30;+(price)#-(RANK)#,percent:20;-(RANK)#,percent:10;"), rankSortClause->toString());
}

END_HA3_NAMESPACE(common);

