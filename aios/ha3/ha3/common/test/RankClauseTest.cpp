#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/test/test.h>
#include <autil/legacy/any.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>
#include <ha3/common/RankClause.h>

using namespace std;

USE_HA3_NAMESPACE(util);
BEGIN_HA3_NAMESPACE(common);

class RankClauseTest : public TESTBASE {
public:
    RankClauseTest();
    ~RankClauseTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, RankClauseTest);


RankClauseTest::RankClauseTest() { 
}

RankClauseTest::~RankClauseTest() { 
}

void RankClauseTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void RankClauseTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(RankClauseTest, testSerializeAndDeserialize) { 
    HA3_LOG(DEBUG, "Begin Test!");
    
    string originalString = "rank_profile:rank.conf";
    FieldBoostDescription fieldBoostDes;
    fieldBoostDes["index1"]["field1"] = 100;
    fieldBoostDes["index2"]["field2"] = 200;

    RankClause rankClause;
    rankClause.setOriginalString(originalString);
    rankClause.setRankProfileName("rank.conf");
    rankClause.setFieldBoostDescription(fieldBoostDes);

    RankClause rankClause2;
    autil::DataBuffer buffer;
    buffer.write(rankClause);
    buffer.read(rankClause2);

    ASSERT_EQ(originalString, rankClause2.getOriginalString());
    ASSERT_EQ(string("rank.conf"), rankClause2.getRankProfileName());
    ASSERT_TRUE(fieldBoostDes ==  rankClause2.getFieldBoostDescription());
}

TEST_F(RankClauseTest, testToString) {
    string requestStr = "rank=rank_profile:DefaultProfile,"
                        "fieldboost:default.TITLE(500);customize.BODY(1000),"
                        "rank_hint:+price";
    RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(requestStr);
    ASSERT_TRUE(requestPtr);
    RankClause* rankClause 
        = requestPtr->getRankClause();
    ASSERT_EQ(string("rankprofilename:DefaultProfile,"
                                "fieldboostdescription:package:[customize:BODY|1000,]"
                                "package:[default:TITLE|500,],"
                                "rankhint:sortfield:price,sortpattern:1"), 
                         rankClause->toString());    
}

END_HA3_NAMESPACE(common);

