#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/QueryLayerClause.h>
#include <ha3/queryparser/ClauseParserContext.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>

using namespace std;
USE_HA3_NAMESPACE(queryparser);
BEGIN_HA3_NAMESPACE(common);

class QueryLayerClauseTest : public TESTBASE {
public:
    QueryLayerClauseTest();
    ~QueryLayerClauseTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, QueryLayerClauseTest);


QueryLayerClauseTest::QueryLayerClauseTest() { 
}

QueryLayerClauseTest::~QueryLayerClauseTest() { 
}

void QueryLayerClauseTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void QueryLayerClauseTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(QueryLayerClauseTest, testSerialize) { 
    HA3_LOG(DEBUG, "Begin Test!");
    ClauseParserContext ctx;
    bool ret = ctx.parseLayerClause("range:cat{1,2,3,[10,20],[99,],66,[,44]}*ends{[555,]},quota:1000;range:cat{2,3,[10,20],[,]},quota:100");
    ASSERT_TRUE(ret);
    QueryLayerClause *layerClause = ctx.stealLayerClause();
    unique_ptr<QueryLayerClause> layerClause_ptr(layerClause);

    autil::DataBuffer db;
    layerClause->serialize(db);
    QueryLayerClause deserializeLayerClause;
    deserializeLayerClause.deserialize(db);
    

    ASSERT_TRUE(layerClause);
    ASSERT_EQ(size_t(2), deserializeLayerClause.getLayerCount());
    LayerDescription *layerDescription = deserializeLayerClause.getLayerDescription(0);
    ASSERT_EQ(uint32_t(1000), layerDescription->getQuota());
    const LayerKeyRange *range = layerDescription->getLayerRange(0);
    ASSERT_EQ(size_t(4), range->values.size());
    ASSERT_EQ(string("cat"), range->attrName);
    ASSERT_EQ(string("1"), range->values[0]);
    ASSERT_EQ(string("2"), range->values[1]);
    ASSERT_EQ(string("3"), range->values[2]);
    ASSERT_EQ(string("66"), range->values[3]);
    ASSERT_EQ(size_t(3), range->ranges.size());
    ASSERT_EQ(string("10"), range->ranges[0].from);
    ASSERT_EQ(string("20"), range->ranges[0].to);
    ASSERT_EQ(string("99"), range->ranges[1].from);
    ASSERT_EQ(string("INFINITE"), range->ranges[1].to);
    ASSERT_EQ(string("INFINITE"), range->ranges[2].from);
    ASSERT_EQ(string("44"), range->ranges[2].to);

    range = layerDescription->getLayerRange(1);
    ASSERT_EQ(string("ends"), range->attrName);
    ASSERT_EQ(size_t(0), range->values.size());
    ASSERT_EQ(size_t(1), range->ranges.size());
    ASSERT_EQ(string("555"), range->ranges[0].from);
    ASSERT_EQ(string("INFINITE"), range->ranges[0].to);
    
    ASSERT_EQ(size_t(2), layerDescription->getLayerRangeCount());
    layerDescription->resizeLayerRange(1);
    ASSERT_EQ(size_t(1), layerDescription->getLayerRangeCount());
    ASSERT_TRUE(NULL != layerDescription->getLayerRange(0));
    ASSERT_TRUE(NULL == layerDescription->getLayerRange(1));


    layerDescription = deserializeLayerClause.getLayerDescription(1);
    ASSERT_EQ(uint32_t(100), layerDescription->getQuota());
    range = layerDescription->getLayerRange(0);
    ASSERT_EQ(size_t(2), range->values.size());
    ASSERT_EQ(string("2"), range->values[0]);
    ASSERT_EQ(string("3"), range->values[1]);
    ASSERT_EQ(size_t(2), range->ranges.size());
    ASSERT_EQ(string("10"), range->ranges[0].from);
    ASSERT_EQ(string("20"), range->ranges[0].to);
    ASSERT_EQ(string("INFINITE"), range->ranges[1].from);
    ASSERT_EQ(string("INFINITE"), range->ranges[1].to);
}

TEST_F(QueryLayerClauseTest, testToString) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string query1 = "layer=range:site_id{1,[10,20],7}*static_score{[100,]},quota:4000;"
                    "range:site_id{5,10},quota:1000";
    RequestPtr requestPtr1 = qrs::RequestCreator::prepareRequest(query1);
    ASSERT_TRUE(requestPtr1);
    QueryLayerClause* queryLayerClause1 = requestPtr1->getQueryLayerClause();
    ASSERT_EQ(string("{keyRanges=site_id[1,7,10:20,];static_score[100:INFINITE,];,"
                                "quota=4000,quotaMode=0}"
                                "{keyRanges=site_id[5,10,];,quota=1000,quotaMode=0}"),
                         queryLayerClause1->toString());
}

END_HA3_NAMESPACE(common);

