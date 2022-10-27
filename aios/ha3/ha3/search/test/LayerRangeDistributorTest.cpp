#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/LayerRangeDistributor.h>
#include <ha3/search/test/LayerMetasConstructor.h>
#include <autil/StringUtil.h>
#include <tr1/functional>

using namespace std;
using namespace autil;

BEGIN_HA3_NAMESPACE(search);

class LayerRangeDistributorTest : public TESTBASE {
public:
    LayerRangeDistributorTest();
    ~LayerRangeDistributorTest();
public:
    void setUp();
    void tearDown();
protected:
    void internalTestInitLayerQuota(
            const std::string &layerMetasStr,
            uint32_t rankSize, 
            const std::string &expectedQuotaStr, 
            const std::string &expectedMaxQuotaStr);
    void internalTestUpdateLayerMetaForNextLayer(
            const std::string &alreadySeekedLayer,
            const std::string &nextLayer, const std::string &result,
            uint32_t quota = 0);
    void internalTestQuotaDistribute(
            const std::string &layerMetasStr, 
            uint32_t quota, const std::string &expectedQuotaStr,
            const std::tr1::function<void (LayerMeta &layerMeta)> &func=LayerRangeDistributor::proportionalLayerQuota);
protected:
    autil::mem_pool::Pool *_pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, LayerRangeDistributorTest);


LayerRangeDistributorTest::LayerRangeDistributorTest() {
    _pool = new autil::mem_pool::Pool(1024);
}

LayerRangeDistributorTest::~LayerRangeDistributorTest() {
    delete _pool;
}

void LayerRangeDistributorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void LayerRangeDistributorTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(LayerRangeDistributorTest, testInitLayerQuota) {
    // one layer
    internalTestInitLayerQuota("param,6", 10, "6", "10");
    // multi layer
    internalTestInitLayerQuota("param,5|param,8", 8, "5,3", "8,3");
    // first layer quota greater than rankSize
    internalTestInitLayerQuota("param,5|param,6", 3, "3,0", "3,0");
    // second layer quota greater than rankSize
    internalTestInitLayerQuota("param,5|param,10", 8, "5,3", "8,3");
}

void LayerRangeDistributorTest::internalTestInitLayerQuota(
        const string &layerMetasStr, uint32_t rankSize,
        const string &expectedQuotaStr, const string &expectedMaxQuotaStr)
{
    LayerMetas layerMetas = LayerMetasConstructor::createLayerMetas(_pool, layerMetasStr);
    LayerRangeDistributor::initLayerQuota(&layerMetas, rankSize);
    vector<uint32_t> expectedQuotas;
    vector<uint32_t> expectedMaxQuotas;
    StringUtil::fromString<uint32_t>(expectedQuotaStr, expectedQuotas, ",");
    StringUtil::fromString<uint32_t>(expectedMaxQuotaStr, expectedMaxQuotas, ",");

    ASSERT_EQ(expectedQuotas.size(), layerMetas.size());
    for (size_t i = 0; i < expectedQuotas.size(); i++) {
        const LayerMeta &layerMeta = layerMetas[i];
        ASSERT_EQ(expectedQuotas[i], layerMeta.quota);
        ASSERT_EQ(expectedMaxQuotas[i], layerMeta.maxQuota);
    }
}

TEST_F(LayerRangeDistributorTest, testUpdateLayerMetaForNextLayer) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        string alreadySeekedLayer = "0,5,1;8,11,1";
        string nextLayer = "4,6,1;7,10,1";
        string result = "param,2;6,6,0;7,7,0";
        internalTestUpdateLayerMetaForNextLayer(alreadySeekedLayer, nextLayer, result);
    }
    {
        string alreadySeekedLayer = "15,22,1;25,27,1";
        string nextLayer = "4,6,1;37,40,1";
        string result = "param,2;4,6,0;37,40,0";
        internalTestUpdateLayerMetaForNextLayer(alreadySeekedLayer, nextLayer, result);
    }
    {
        string alreadySeekedLayer = "1,3,1;4,5,1;6,7,1;8,11,1";
        string nextLayer = "2,9,1;10,13,1";
        string result = "param,2;12,13,0";
        internalTestUpdateLayerMetaForNextLayer(alreadySeekedLayer, nextLayer, result);
    }
    {
        string alreadySeekedLayer = "1,3,1;5,6,1;10,11,1;15,16,1";
        string nextLayer = "2,12,1;14,16,1";
        string result = "param,2;4,4,0;7,9,0;12,12,0;14,14,0";
        internalTestUpdateLayerMetaForNextLayer(alreadySeekedLayer, nextLayer, result);
    }
    {
        string alreadySeekedLayer = "3,5,1;8,11,1";
        string nextLayer = "0,6,1;7,99,1";
        string result = "param,2;0,2,0;6,6,0;7,7,0;12,99,0";
        internalTestUpdateLayerMetaForNextLayer(alreadySeekedLayer, nextLayer, result);
    }
    {
        string alreadySeekedLayer = "3,5,1;8,11,1";
        string nextLayer = "0,6,1;7,100,1";
        string result = "param,2;0,2,0;6,6,0;7,7,0;12,99,0";
        internalTestUpdateLayerMetaForNextLayer(alreadySeekedLayer, nextLayer, result);
    }
    {
        string alreadySeekedLayer = "0,5,1;6,99,1";
        string nextLayer = "0,100,1";
        string result = "param,1";
        internalTestUpdateLayerMetaForNextLayer(alreadySeekedLayer, nextLayer, result, 0);
    }
    {
        string alreadySeekedLayer = "0,5,1;6,88,1";
        string nextLayer = "0,100,1";
        string result = "param,1;89,99,0";
        internalTestUpdateLayerMetaForNextLayer(alreadySeekedLayer, nextLayer, result);
    }
    {
        string alreadySeekedLayer = "0,5,1;6,88,1";
        string nextLayer = "0,100,1";
        string result = "param,1;89,99,0";
        internalTestUpdateLayerMetaForNextLayer(alreadySeekedLayer, nextLayer, result);
    }
    {
        string alreadySeekedLayer = "0,88,2";
        string nextLayer = "0,7,5;15,50,4;55,60,3;70,80,1;85,100,333";
        string result = "param,346;89,99,0";
        internalTestUpdateLayerMetaForNextLayer(alreadySeekedLayer, nextLayer, result);
    }
    {
        string alreadySeekedLayer = "2,9,2;11,20,2;25,40,2;48,60,2;70,80,2";
        string nextLayer = "0,5,2;7,15,2;18,75,2;77,79,2;80,99,2";
        string result = "param,10;0,1,0;10,10,0;21,24,0;41,47,0;61,69,0;81,99,0";
        internalTestUpdateLayerMetaForNextLayer(alreadySeekedLayer, nextLayer, result);
    }
    {
        string alreadySeekedLayer = "0,5,1;6,88,1";
        string nextLayer = "0,5,1;6,88,1";
        string result = "param,2";
        internalTestUpdateLayerMetaForNextLayer(alreadySeekedLayer, nextLayer, result, 0);
    }
    {
        string alreadySeekedLayer = "";
        string nextLayer = "param,2147483647,QM_PER_LAYER;10,10000000,0";
        string result = "param,2147483647,QM_PER_LAYER;10,99,0";
        internalTestUpdateLayerMetaForNextLayer(alreadySeekedLayer, nextLayer, result, 0);
    }
}

TEST_F(LayerRangeDistributorTest, testUpdateLayerMetaForNextLayerWithQuotaType) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        string alreadySeekedLayer = "0,5,1;8,11,1";
        string nextLayer = "0#4,6,1;7,10,1";
        string result = "param,2;6,6,0;7,7,0";
        internalTestUpdateLayerMetaForNextLayer(alreadySeekedLayer, nextLayer, result);
    }
    {
        string alreadySeekedLayer = "0,5,1;8,11,1";
        string nextLayer = "1#4,6,1;7,10,1";
        string result = "1#param,2;6,6,0;7,7,0";
        internalTestUpdateLayerMetaForNextLayer(alreadySeekedLayer, nextLayer, result);
    }    
}

TEST_F(LayerRangeDistributorTest, testUpdateLayerQuota) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        string alreadySeekedLayer = "0,5,1;8,11,1";
        string nextLayer = "4,6,1;9,10,1";
        string result = "param,2;6,6,0";
        internalTestUpdateLayerMetaForNextLayer(alreadySeekedLayer, nextLayer, result);
    }
    {
        string alreadySeekedLayer = "0,5,1;8,99,1";
        string nextLayer = "4,6,1;9,23,1;50,99,2";
        string result = "param,4;6,6,0";
        internalTestUpdateLayerMetaForNextLayer(alreadySeekedLayer, nextLayer, result);
    }
    {
        string alreadySeekedLayer = "0,5,1;18,99,1";
        string nextLayer = "0,4,1;10,19,1;50,99,2";
        string result = "param,4;10,17,0";
        internalTestUpdateLayerMetaForNextLayer(alreadySeekedLayer, nextLayer, result);
    }
}

TEST_F(LayerRangeDistributorTest, testEmptyLayer) {
    LayerMetas layerMetas = LayerMetasConstructor::createLayerMetas(
            _pool, "|1,2,3");
    ASSERT_EQ(size_t(2), layerMetas.size());
    LayerRangeDistributor distribute(&layerMetas, _pool, 10, 100);
    uint32_t layer = 0;
    LayerMeta *layerMeta = distribute.getCurLayer(layer);
    ASSERT_EQ(uint32_t(1), layer);
    ASSERT_TRUE(layerMeta);
}

TEST_F(LayerRangeDistributorTest, testProportionalLayerQuota) {
    internalTestQuotaDistribute("1,4,0;5,7,0", 0, "0,0");

    internalTestQuotaDistribute("1,4,0;5,7,0", 11, "7,4");

    internalTestQuotaDistribute("1,4,0", 11, "11");

    internalTestQuotaDistribute("1,4,0;5,7,0", 11, "7,4");

    internalTestQuotaDistribute("7,10,0;12,14,0", 11, "7,4");

    internalTestQuotaDistribute("1,4,0;5,7,0;7,10,0;12,14,0", 11, "4,2,3,2");
}

TEST_F(LayerRangeDistributorTest, testAverageLayerQuota) {
    internalTestQuotaDistribute("1,4,0;5,7,0", 0, "0,0", LayerRangeDistributor::averageLayerQuota);
    internalTestQuotaDistribute("1,4,0;5,7,0", 11, "6,5", LayerRangeDistributor::averageLayerQuota);
    internalTestQuotaDistribute("1,4,0", 11, "11", LayerRangeDistributor::averageLayerQuota);
    internalTestQuotaDistribute("7,10,0;12,14,0", 11, "6,5", LayerRangeDistributor::averageLayerQuota);
    internalTestQuotaDistribute("1,4,0;5,7,0;7,10,0;12,14,0", 11, "5,2,2,2", LayerRangeDistributor::averageLayerQuota);
}

void LayerRangeDistributorTest::internalTestQuotaDistribute(
        const string &layerMetaStr, uint32_t quota, const string &expectedQuotaStr,
        const std::tr1::function<void (LayerMeta &layerMeta)> &func) 
{
    LayerMeta layerMeta = LayerMetasConstructor::createLayerMeta(_pool, layerMetaStr);
    layerMeta.quota = quota;
    func(layerMeta);
    vector<uint32_t> expectedQuotas;
    StringUtil::fromString<uint32_t>(expectedQuotaStr, expectedQuotas, ",");
    ASSERT_EQ(expectedQuotas.size(), layerMeta.size());
    for (size_t i = 0; i < expectedQuotas.size(); i++) {
        ASSERT_EQ(expectedQuotas[i], layerMeta[i].quota);
    }
    ASSERT_EQ(0u, layerMeta.quota);
}

TEST_F(LayerRangeDistributorTest, testMoveToNextLayerNormal) {
    LayerMetas layerMetas = LayerMetasConstructor::createLayerMetas(_pool, 
            "param,11;1,4,0;5,7,0|param,5;8,9,0;11,13,0");
    LayerRangeDistributor distributor(&layerMetas, _pool, 15u, 12u);
    ASSERT_EQ(0u, distributor._leftQuota);
    uint32_t layerCursor;
    LayerMeta *layerMeta = distributor.getCurLayer(layerCursor);
    ASSERT_EQ(0u, layerCursor);
    ASSERT_EQ(size_t(0), distributor._seekedRange.size());
    ASSERT_EQ(7u, (*layerMeta)[0].quota);
    ASSERT_EQ(4u, (*layerMeta)[1].quota);
    ASSERT_EQ(0, distributor._curLayerCursor);
    ASSERT_TRUE(distributor.hasNextLayer());

    distributor.moveToNextLayer(6);
    ASSERT_EQ(0u, distributor._leftQuota);
    layerMeta = distributor.getCurLayer(layerCursor);
    ASSERT_EQ(1u, layerCursor);
    ASSERT_EQ(size_t(2), distributor._seekedRange.size());
    ASSERT_EQ(3u, (*layerMeta)[0].quota);
    ASSERT_EQ(4u, (*layerMeta)[1].quota);
    ASSERT_EQ(1, distributor._curLayerCursor);
    ASSERT_TRUE(distributor.hasNextLayer());

    distributor.moveToNextLayer(2);
    ASSERT_EQ(2, distributor._curLayerCursor);
    ASSERT_TRUE(!distributor.hasNextLayer());
}

TEST_F(LayerRangeDistributorTest, testMoveToNextLayerEmptyLayer) {
    LayerMetas layerMetas = LayerMetasConstructor::createLayerMetas(_pool, 
            "param,UNLIMITED|param,2;1,4,0;5,7,0");
    LayerRangeDistributor distributor(&layerMetas, _pool, 15u, 12u);
    ASSERT_EQ(0u, distributor._leftQuota);
    uint32_t layerCursor;
    LayerMeta *layerMeta = distributor.getCurLayer(layerCursor);
    ASSERT_EQ(1u, layerCursor);
    ASSERT_EQ(size_t(0), distributor._seekedRange.size());
    ASSERT_EQ(1, distributor._curLayerCursor);
    ASSERT_EQ(size_t(2), layerMeta->size());
    ASSERT_EQ(7u, (*layerMeta)[0].quota);
    ASSERT_EQ(5u, (*layerMeta)[1].quota);
    ASSERT_TRUE(distributor.hasNextLayer());

    distributor.moveToNextLayer(0);
    ASSERT_TRUE(!distributor.hasNextLayer());    
}

TEST_F(LayerRangeDistributorTest, testMoveToNextLayerWithCover) {
    LayerMetas layerMetas = LayerMetasConstructor::createLayerMetas(_pool, 
            "param,5;1,4,0;6,7,0|param,5;2,5,0;6,9,0");
    LayerRangeDistributor distributor(&layerMetas, _pool, 15u, 10u);
    ASSERT_EQ(0u, distributor._leftQuota);
    uint32_t layerCursor;
    LayerMeta *layerMeta = distributor.getCurLayer(layerCursor);
    ASSERT_EQ(0u, layerCursor);
    ASSERT_EQ(size_t(0), distributor._seekedRange.size());
    ASSERT_EQ(0, distributor._curLayerCursor);
    ASSERT_EQ(size_t(2), layerMeta->size());
    ASSERT_EQ(4u, (*layerMeta)[0].quota);
    ASSERT_EQ(1u, (*layerMeta)[1].quota);
    ASSERT_TRUE(distributor.hasNextLayer());
    
    (*layerMeta)[0].nextBegin = 5;
    (*layerMeta)[1].nextBegin = 8;

    distributor.moveToNextLayer(4);
    ASSERT_EQ(0u, distributor._leftQuota);
    layerMeta = distributor.getCurLayer(layerCursor);
    ASSERT_EQ(1u, layerCursor);
    ASSERT_EQ(size_t(2), distributor._seekedRange.size());
    ASSERT_EQ(1, distributor._curLayerCursor);
    ASSERT_EQ(size_t(2), layerMeta->size());
    ASSERT_EQ(5, (*layerMeta)[0].begin);
    ASSERT_EQ(5, (*layerMeta)[0].end);
    ASSERT_EQ(8, (*layerMeta)[1].begin);
    ASSERT_EQ(9, (*layerMeta)[1].end);
    ASSERT_EQ(3u, (*layerMeta)[0].quota);
    ASSERT_EQ(6u, (*layerMeta)[1].quota);
    ASSERT_TRUE(distributor.hasNextLayer());

    distributor.moveToNextLayer(2);
    ASSERT_TRUE(!distributor.hasNextLayer());
}

TEST_F(LayerRangeDistributorTest, testMoveToNextLayerWithQuotaMode) {
    LayerMetas layerMetas = LayerMetasConstructor::createLayerMetas(_pool,
            "param,10,QM_PER_LAYER;1,4,0;5,7,0|8,9,0;10,12,0|param,0,QM_PER_LAYER;14,15,0");
    LayerRangeDistributor distributor(&layerMetas, _pool, 20u, 10u);
    ASSERT_EQ(0u, distributor._leftQuota);

    uint32_t layerCursor;
    LayerMeta *layerMeta = distributor.getCurLayer(layerCursor);
    ASSERT_EQ(0u, layerCursor);
    ASSERT_EQ(size_t(0), distributor._seekedRange.size());
    ASSERT_EQ(0, distributor._curLayerCursor);
    ASSERT_EQ(10u, layerMeta->quota);
    ASSERT_EQ(size_t(2), layerMeta->size());
    ASSERT_EQ(0u, (*layerMeta)[0].quota);
    ASSERT_EQ(0u, (*layerMeta)[1].quota);
    ASSERT_TRUE(distributor.hasNextLayer());

    distributor.moveToNextLayer(5);
    ASSERT_EQ(0u, distributor._leftQuota);
    layerMeta = distributor.getCurLayer(layerCursor);
    ASSERT_EQ(1u, layerCursor);
    ASSERT_EQ(size_t(2), distributor._seekedRange.size());
    ASSERT_EQ(1, distributor._curLayerCursor);
    ASSERT_EQ(size_t(2), layerMeta->size());
    ASSERT_EQ(2u, (*layerMeta)[0].quota);
    ASSERT_EQ(3u, (*layerMeta)[1].quota);
    ASSERT_TRUE(distributor.hasNextLayer());

    distributor.moveToNextLayer(2);
    ASSERT_EQ(0u, distributor._leftQuota);
    layerMeta = distributor.getCurLayer(layerCursor);
    ASSERT_EQ(2u, layerCursor);
    ASSERT_EQ(size_t(4), distributor._seekedRange.size());
    ASSERT_EQ(2, distributor._curLayerCursor);
    ASSERT_EQ(2u, layerMeta->quota);
    ASSERT_EQ(size_t(1), layerMeta->size());
    ASSERT_EQ(0u, (*layerMeta)[0].quota);
    ASSERT_TRUE(distributor.hasNextLayer());

    distributor.moveToNextLayer(1);
    ASSERT_TRUE(!distributor.hasNextLayer());
}

void LayerRangeDistributorTest::internalTestUpdateLayerMetaForNextLayer(
        const string &alreadySeekedLayerStr, const string &nextLayerStr,
        const string &resultStr, uint32_t quota)
{
    LayerMeta curLayer = LayerMetasConstructor::createLayerMeta(_pool,
            alreadySeekedLayerStr);
    for (size_t i = 0; i < curLayer.size(); ++i) {
        curLayer[i].nextBegin = curLayer[i].end + 1;
    }
    LayerMeta nextLayer = LayerMetasConstructor::createLayerMeta(_pool,
            nextLayerStr);
    LayerMeta targetLayer = LayerMetasConstructor::createLayerMeta(_pool,
            resultStr);

    uint32_t leftQuota = 0;

    nextLayer = LayerRangeDistributor::updateLayerMetaForNextLayer(_pool,
            curLayer, nextLayer, leftQuota, 100);

    ASSERT_EQ(targetLayer.quota, nextLayer.quota);
    ASSERT_EQ(targetLayer.quotaMode, nextLayer.quotaMode);
    ASSERT_EQ(targetLayer.quotaType, nextLayer.quotaType);    

    if (!(targetLayer.size() == nextLayer.size())) {
        HA3_LOG(ERROR, "targetLayer is [%s]", targetLayer.toString().c_str());
        assert(false);
    }
    for (size_t i = 0; i < targetLayer.size(); ++i) {
        ASSERT_EQ(targetLayer[i], nextLayer[i]);
    }
    ASSERT_EQ(quota, leftQuota);
}

TEST_F(LayerRangeDistributorTest, testReverseRange) {
    LayerMeta curLayer = LayerMetasConstructor::createLayerMeta(_pool, "1,2,0;4,5,0");
    curLayer[0].nextBegin = 3;
    LayerMeta result = LayerRangeDistributor::reverseRange(_pool, curLayer, 10);
    ASSERT_EQ(size_t(3), result.size());
    ASSERT_EQ(0, result[0].begin);
    ASSERT_EQ(0, result[0].end);
    ASSERT_EQ(3, result[1].begin);
    ASSERT_EQ(3, result[1].end);
    ASSERT_EQ(4, result[2].begin);
    ASSERT_EQ(9, result[2].end);
}

END_HA3_NAMESPACE(search);

