#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/ClauseParserContext.h>

using namespace std;

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(queryparser);

class LayerClauseParserTest : public TESTBASE {
public:
    LayerClauseParserTest();
    ~LayerClauseParserTest();
public:
    void setUp();
    void tearDown();
protected:
    ClauseParserContext *_ctx;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(queryparser, LayerClauseParserTest);


LayerClauseParserTest::LayerClauseParserTest() { 
}

LayerClauseParserTest::~LayerClauseParserTest() { 
}

void LayerClauseParserTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _ctx = new ClauseParserContext;
}

void LayerClauseParserTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    delete _ctx;
}

TEST_F(LayerClauseParserTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    bool ret = _ctx->parseLayerClause("range:cat{1,2,3,[10,20],[99,],66,[,44]}*ends{[555,]},quota:1000;range:cat{2,3,[10,20],[,],[,)},quota:100#2;range:%other");
    ASSERT_TRUE(ret);
    QueryLayerClause *layerClause = _ctx->stealLayerClause();
    unique_ptr<QueryLayerClause> layerClause_ptr(layerClause);

    ASSERT_TRUE(layerClause);
    ASSERT_EQ(size_t(3), layerClause->getLayerCount());
    const LayerDescription *layerDescription = layerClause->getLayerDescription(0);
    ASSERT_EQ(uint32_t(1000), layerDescription->getQuota());
    const LayerKeyRange *range = layerDescription->getLayerRange(0);
    ASSERT_EQ(QT_PROPOTION, layerDescription->getQuotaType());
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

    layerDescription = layerClause->getLayerDescription(1);
    ASSERT_EQ(uint32_t(100), layerDescription->getQuota());
    ASSERT_EQ(QT_QUOTA, layerDescription->getQuotaType());
    range = layerDescription->getLayerRange(0);
    ASSERT_EQ(size_t(2), range->values.size());
    ASSERT_EQ(string("2"), range->values[0]);
    ASSERT_EQ(string("3"), range->values[1]);
    ASSERT_EQ(size_t(3), range->ranges.size());
    ASSERT_EQ(string("10"), range->ranges[0].from);
    ASSERT_EQ(string("20"), range->ranges[0].to);
    ASSERT_EQ(string("INFINITE"), range->ranges[1].from);
    ASSERT_EQ(string("INFINITE"), range->ranges[1].to);   
    ASSERT_EQ(string("INFINITE"), range->ranges[1].from);
    ASSERT_EQ(string("INFINITE"), range->ranges[1].to);   

    layerDescription = layerClause->getLayerDescription(2);
    ASSERT_EQ(uint32_t(0), layerDescription->getQuota());
    ASSERT_EQ(QT_PROPOTION, layerDescription->getQuotaType());
    range = layerDescription->getLayerRange(0);
    ASSERT_EQ(string("%other"), range->attrName);
    ASSERT_EQ(size_t(0), range->values.size());
    ASSERT_EQ(size_t(0), range->ranges.size());
}


TEST_F(LayerClauseParserTest, testInvalidQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_TRUE(!_ctx->parseLayerClause(""));
    ASSERT_TRUE(!_ctx->parseLayerClause("range:cat{2,[10,20]},quotsa:100"));
    ASSERT_TRUE(!_ctx->parseLayerClause("range:cat{1}*ends[555,]}"));
    ASSERT_TRUE(!_ctx->parseLayerClause("range:cat{66 32}"));
    ASSERT_TRUE(!_ctx->parseLayerClause("range:cat{66,abx}"));
    ASSERT_TRUE(!_ctx->parseLayerClause("range:cat{66 2a}"));
}

TEST_F(LayerClauseParserTest, testValidQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_TRUE(_ctx->parseLayerClause("range:cat{0.3,[10,20]},quota:100"));
    ASSERT_TRUE(_ctx->parseLayerClause("range:cat{[-10,20]},quota:100"));
    ASSERT_TRUE(_ctx->parseLayerClause("range:cat{[10,20]}"));
    ASSERT_TRUE(_ctx->parseLayerClause("range:cat{[10,     20]}"));
    ASSERT_TRUE(_ctx->parseLayerClause("quota:100;quota:100"));
    // ASSERT_TRUE(_ctx->parseLayerClause(";;"));
}

TEST_F(LayerClauseParserTest, testKeyWord) {
    bool ret = _ctx->parseLayerClause("range:%docid{[10,20]}");
    ASSERT_TRUE(ret);
    QueryLayerClause *layerClause = _ctx->stealLayerClause();
    ASSERT_TRUE(layerClause);
    unique_ptr<QueryLayerClause> layerClause_ptr(layerClause); 
    ASSERT_EQ((size_t)1, layerClause->getLayerCount());
    const LayerDescription *layerDescription = layerClause->getLayerDescription(0);
    ASSERT_EQ(size_t(1), layerDescription->getLayerRangeCount());
    const LayerKeyRange *range = layerDescription->getLayerRange(0);
    ASSERT_EQ(string("%docid"), range->attrName);
}

END_HA3_NAMESPACE(queryparser);

