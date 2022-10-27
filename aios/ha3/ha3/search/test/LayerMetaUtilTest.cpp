#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/LayerMetaUtil.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3/search/LayerMetasCreator.h>
#include <autil/StringUtil.h>
#include <ha3/common/QueryLayerClause.h>
#include <ha3/queryparser/ClauseParserContext.h>

using namespace autil;
using namespace std;

IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index);

USE_HA3_NAMESPACE(queryparser);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);

class LayerMetaUtilTest : public TESTBASE {
public:
    LayerMetaUtilTest();
    ~LayerMetaUtilTest();
public:
    void setUp();
    void tearDown();
protected:
    DocIdRangeMeta createDocIdRangeWithNextBegin(docid_t begin,
            docid_t end, docid_t nextBegin);
private:
    QueryLayerClause* initQueryLayerClause(const string &layerStr) {
        ClauseParserContext ctx;
        if (!ctx.parseLayerClause(layerStr.c_str())) {
            HA3_LOG(ERROR, "msg is [%s]", layerStr.c_str());
            assert(false);
        }
        QueryLayerClause *layerClause = ctx.stealLayerClause();
        return layerClause;
    }

protected:
    autil::mem_pool::Pool *_pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, LayerMetaUtilTest);


LayerMetaUtilTest::LayerMetaUtilTest() {
    _pool = new autil::mem_pool::Pool(1024);
}

LayerMetaUtilTest::~LayerMetaUtilTest() {
    delete _pool;
}

void LayerMetaUtilTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void LayerMetaUtilTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(LayerMetaUtilTest, testReverseWithEnd) {
    HA3_LOG(DEBUG, "Begin Test!");

    LayerMeta layerMeta(_pool);
    LayerMeta resultMeta(_pool);
    layerMeta.push_back(DocIdRangeMeta(0,3));
    layerMeta.push_back(DocIdRangeMeta(4,5));
    layerMeta.push_back(DocIdRangeMeta(4,5));
    layerMeta.push_back(DocIdRangeMeta(8,11));
    layerMeta.push_back(DocIdRangeMeta(9,13));
    layerMeta.push_back(DocIdRangeMeta(15,18));
    resultMeta = LayerMetaUtil::reverseRangeWithEnd(_pool, layerMeta, 20);

    ASSERT_EQ(size_t(3), resultMeta.size());
    ASSERT_EQ(DocIdRangeMeta(6, 7), resultMeta[0]);
    ASSERT_EQ(DocIdRangeMeta(14, 14), resultMeta[1]);
    ASSERT_EQ(DocIdRangeMeta(19, 19), resultMeta[2]);
}

TEST_F(LayerMetaUtilTest, testReverseWithEndEmpty) {
    LayerMeta layerMeta(_pool);
    LayerMeta resultMeta(_pool);
    resultMeta = LayerMetaUtil::reverseRange(_pool, layerMeta, 20);

    ASSERT_EQ(size_t(1), resultMeta.size());
    ASSERT_EQ(DocIdRangeMeta(0, 19), resultMeta[0]);
}

TEST_F(LayerMetaUtilTest, testReverseWithNextBegin) {
    LayerMeta layerMeta(_pool);
    LayerMeta resultMeta(_pool);
    layerMeta.push_back(createDocIdRangeWithNextBegin(0, 3, 2));
    layerMeta.push_back(createDocIdRangeWithNextBegin(4, 5, 5));
    layerMeta.push_back(createDocIdRangeWithNextBegin(4, 5, 4));
    layerMeta.push_back(createDocIdRangeWithNextBegin(8, 11, 10));
    layerMeta.push_back(createDocIdRangeWithNextBegin(9, 13, 11));
    layerMeta.push_back(createDocIdRangeWithNextBegin(15, 18, 17));
    resultMeta = LayerMetaUtil::reverseRange(_pool, layerMeta, 20);

    ASSERT_EQ(size_t(4), resultMeta.size());
    ASSERT_EQ(DocIdRangeMeta(2, 3), resultMeta[0]);
    ASSERT_EQ(DocIdRangeMeta(4, 7), resultMeta[1]);
    ASSERT_EQ(DocIdRangeMeta(11, 14), resultMeta[2]);
    ASSERT_EQ(DocIdRangeMeta(17, 19), resultMeta[3]);

}

TEST_F(LayerMetaUtilTest, testReverseWithNextBeginEmpty) {
    LayerMeta layerMeta(_pool);
    LayerMeta resultMeta(_pool);
    resultMeta = LayerMetaUtil::reverseRange(_pool, layerMeta, 20);

    ASSERT_EQ(size_t(1), resultMeta.size());
    ASSERT_EQ(DocIdRangeMeta(0, 19), resultMeta[0]);

}

TEST_F(LayerMetaUtilTest, testAccumulateRangesWithOneLayerEmpty) {
    IndexPartitionReaderPtr reader(new FakeIndexPartitionReader);

    vector<IndexPartitionReaderPtr> indexPartReaders;
    indexPartReaders.push_back(reader);
    IndexPartitionReaderWrapper wrapper(NULL, NULL, indexPartReaders);

    LayerMetasCreator creator;
    creator.init(_pool, &wrapper, 0);
    LayerMeta usedLayer(_pool);
    LayerMeta layer(_pool);
    LayerMeta resultLayer(_pool);
    layer.push_back(DocIdRangeMeta(0, 3));
    layer.push_back(DocIdRangeMeta(3, 5));
    layer.push_back(DocIdRangeMeta(5, 7));
    layer.push_back(DocIdRangeMeta(10, 12));
    layer.push_back(DocIdRangeMeta(10, 12));
    layer.push_back(DocIdRangeMeta(13, 14));
    layer.push_back(DocIdRangeMeta(16, 18));
    resultLayer = LayerMetaUtil::accumulateRanges(_pool, layer, usedLayer);

    ASSERT_EQ(size_t(4), resultLayer.size());
    ASSERT_EQ(DocIdRangeMeta(0, 7), resultLayer[0]);
    ASSERT_EQ(DocIdRangeMeta(10, 12), resultLayer[1]);
    ASSERT_EQ(DocIdRangeMeta(13, 14), resultLayer[2]);
    ASSERT_EQ(DocIdRangeMeta(16, 18), resultLayer[3]);
}

TEST_F(LayerMetaUtilTest, testAccumulateRanges) {
    IndexPartitionReaderPtr reader(new FakeIndexPartitionReader);

    vector<IndexPartitionReaderPtr> indexPartReaders;
    indexPartReaders.push_back(reader);
    IndexPartitionReaderWrapper wrapper(NULL, NULL, indexPartReaders);

    LayerMetasCreator creator;
    creator.init(_pool, &wrapper, 0);
    LayerMeta usedLayer(_pool);
    LayerMeta layer(_pool);
    LayerMeta resultLayer(_pool);
    layer.push_back(DocIdRangeMeta(0, 3));
    layer.push_back(DocIdRangeMeta(3, 5));
    layer.push_back(DocIdRangeMeta(5, 7));
    layer.push_back(DocIdRangeMeta(10, 12));
    usedLayer.push_back(DocIdRangeMeta(10, 12));
    usedLayer.push_back(DocIdRangeMeta(13, 14));
    usedLayer.push_back(DocIdRangeMeta(16, 18));
    resultLayer = LayerMetaUtil::accumulateRanges(_pool, layer, usedLayer);

    ASSERT_EQ(size_t(4), resultLayer.size());
    ASSERT_EQ(DocIdRangeMeta(0, 7), resultLayer[0]);
    ASSERT_EQ(DocIdRangeMeta(10, 12), resultLayer[1]);
    ASSERT_EQ(DocIdRangeMeta(13, 14), resultLayer[2]);
    ASSERT_EQ(DocIdRangeMeta(16, 18), resultLayer[3]);
}

DocIdRangeMeta LayerMetaUtilTest::createDocIdRangeWithNextBegin(docid_t begin,
        docid_t end, docid_t nextBegin) {
    DocIdRangeMeta rangeMeta(begin, end);
    rangeMeta.nextBegin = nextBegin;
    return rangeMeta;
}

TEST_F(LayerMetaUtilTest, testSplitLayerMetas) {
    //跨区间的segment范围indexlib还不支持切分，没有构造这种case
    {
        //seg1:0~99, 分4路，不够分
        FakeIndex fakeIndex;
        fakeIndex.segmentDocCounts = "100";
        IndexPartitionReaderWrapperPtr wrapper =
            FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
        LayerMetas inputLayerMetas(_pool);
        LayerMeta lyrMeta(_pool);
        lyrMeta.quota = 100;
        lyrMeta.maxQuota = 99999;
        lyrMeta.quotaMode = QM_PER_LAYER;
        lyrMeta.needAggregate = false;
        lyrMeta.quotaType = QT_AVERAGE;
        lyrMeta.push_back(DocIdRangeMeta(0, 1));

        inputLayerMetas.push_back(lyrMeta);
        size_t wayCount = 4;
        vector<LayerMetasPtr> outLayerMetas;
        ASSERT_TRUE(LayerMetaUtil::splitLayerMetas(_pool, inputLayerMetas,
                        wrapper, wayCount, outLayerMetas));
        ASSERT_EQ(wayCount, outLayerMetas.size());
        auto outLayerMetas1 = outLayerMetas[0];
        auto outLayerMetas2 = outLayerMetas[1];
        auto outLayerMetas3 = outLayerMetas[2];
        auto outLayerMetas4 = outLayerMetas[3];
        ASSERT_EQ(1u, outLayerMetas1->size());
        ASSERT_EQ(DocIdRangeMeta(0, 1), (*outLayerMetas1)[0][0]);
        ASSERT_EQ(lyrMeta.quota, (*outLayerMetas1)[0].quota);
        ASSERT_EQ(lyrMeta.maxQuota, (*outLayerMetas1)[0].maxQuota);
        ASSERT_EQ(lyrMeta.quotaMode, (*outLayerMetas1)[0].quotaMode);
        ASSERT_EQ(lyrMeta.needAggregate, (*outLayerMetas1)[0].needAggregate);
        ASSERT_EQ(lyrMeta.quotaType, (*outLayerMetas1)[0].quotaType);
        ASSERT_EQ(1u, outLayerMetas2->size());
        ASSERT_EQ(0u, (*outLayerMetas2)[0].size());
        ASSERT_EQ(1u, outLayerMetas3->size());
        ASSERT_EQ(0u, (*outLayerMetas3)[0].size());
        ASSERT_EQ(1u, outLayerMetas4->size());
        ASSERT_EQ(0u, (*outLayerMetas4)[0].size());
    }
    {
        //seg1:0~99
        FakeIndex fakeIndex;
        fakeIndex.segmentDocCounts = "100";
        IndexPartitionReaderWrapperPtr wrapper =
            FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
        LayerMetas inputLayerMetas(_pool);
        LayerMeta lyrMeta(_pool);
        lyrMeta.quota = 100;
        lyrMeta.quota = 100;
        lyrMeta.maxQuota = 99999;
        lyrMeta.quotaMode = QM_PER_LAYER;
        lyrMeta.needAggregate = false;
        lyrMeta.quotaType = QT_AVERAGE;
        lyrMeta.push_back(DocIdRangeMeta(0, 99));
        inputLayerMetas.push_back(lyrMeta);
        size_t wayCount = 2;
        vector<LayerMetasPtr> outLayerMetas;
        ASSERT_TRUE(LayerMetaUtil::splitLayerMetas(_pool, inputLayerMetas, wrapper,
                        wayCount, outLayerMetas));
        ASSERT_EQ(wayCount, outLayerMetas.size());
        auto outLayerMetas1 = outLayerMetas[0];
        auto outLayerMetas2 = outLayerMetas[1];
        ASSERT_EQ(1u, outLayerMetas1->size());
        ASSERT_EQ(DocIdRangeMeta(0, 49), (*outLayerMetas1)[0][0]);
        ASSERT_EQ(lyrMeta.quota, (*outLayerMetas1)[0].quota);
        ASSERT_EQ(lyrMeta.maxQuota, (*outLayerMetas1)[0].maxQuota);
        ASSERT_EQ(lyrMeta.quotaMode, (*outLayerMetas1)[0].quotaMode);
        ASSERT_EQ(lyrMeta.needAggregate, (*outLayerMetas1)[0].needAggregate);
        ASSERT_EQ(lyrMeta.quotaType, (*outLayerMetas1)[0].quotaType);
        ASSERT_EQ(1u, outLayerMetas2->size());
        ASSERT_EQ(DocIdRangeMeta(50, 99), (*outLayerMetas2)[0][0]);
        ASSERT_EQ(lyrMeta.quota, (*outLayerMetas2)[0].quota);
        ASSERT_EQ(lyrMeta.maxQuota, (*outLayerMetas2)[0].maxQuota);
        ASSERT_EQ(lyrMeta.quotaMode, (*outLayerMetas2)[0].quotaMode);
        ASSERT_EQ(lyrMeta.needAggregate, (*outLayerMetas2)[0].needAggregate);
        ASSERT_EQ(lyrMeta.quotaType, (*outLayerMetas2)[0].quotaType);
    }
    {
        //seg1:0~99, seg2:100~199, seg3:200~249 realtime
        FakeIndex fakeIndex;
        fakeIndex.segmentDocCounts = "100,100|50";
        IndexPartitionReaderWrapperPtr wrapper =
            FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
        LayerMetas inputLayerMetas(_pool);
        LayerMeta lyrMeta1(_pool), lyrMeta2(_pool);
        lyrMeta1.quota = 100;
        lyrMeta2.quota = 200;
        lyrMeta1.push_back(DocIdRangeMeta(0, 59));
        lyrMeta1.push_back(DocIdRangeMeta(100, 149));
        lyrMeta2.push_back(DocIdRangeMeta(150, 179));
        lyrMeta2.push_back(DocIdRangeMeta(200, 249));
        inputLayerMetas.push_back(lyrMeta1);
        inputLayerMetas.push_back(lyrMeta2);
        size_t wayCount = 2;
        vector<LayerMetasPtr> outLayerMetas;
        ASSERT_TRUE(LayerMetaUtil::splitLayerMetas(_pool, inputLayerMetas, wrapper,
                        wayCount, outLayerMetas));
        ASSERT_EQ(wayCount, outLayerMetas.size());
        auto outLayerMetas1 = outLayerMetas[0];
        auto outLayerMetas2 = outLayerMetas[1];
        ASSERT_EQ(2u, outLayerMetas1->size());
        ASSERT_EQ(DocIdRangeMeta(0, 59), (*outLayerMetas1)[0][0]);
        ASSERT_EQ(DocIdRangeMeta(150,164), (*outLayerMetas1)[1][0]);
        ASSERT_EQ(2u, outLayerMetas2->size());
        ASSERT_EQ(DocIdRangeMeta(100, 149), (*outLayerMetas2)[0][0]);
        ASSERT_EQ(DocIdRangeMeta(165, 179), (*outLayerMetas2)[1][0]);
        ASSERT_EQ(DocIdRangeMeta(200, 249), (*outLayerMetas2)[1][1]);
    }
    {   // 这个case范围有交叉，目前行为不是较优解，待indexlib优化
        //seg1:0~49, seg2:50~99, seg3:100~149, seg4:150~199, seg5:200~249 realtime
        FakeIndex fakeIndex;
        fakeIndex.segmentDocCounts = "50,50,50,50|50";
        IndexPartitionReaderWrapperPtr wrapper =
            FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
        LayerMetas inputLayerMetas(_pool);
        LayerMeta lyrMeta1(_pool);
        lyrMeta1.quota = 100;
        lyrMeta1.push_back(DocIdRangeMeta(0, 79));
        lyrMeta1.push_back(DocIdRangeMeta(50, 189));
        lyrMeta1.push_back(DocIdRangeMeta(100, 199));
        inputLayerMetas.push_back(lyrMeta1);
        size_t wayCount = 2;
        vector<LayerMetasPtr> outLayerMetas;
        ASSERT_TRUE(LayerMetaUtil::splitLayerMetas(_pool, inputLayerMetas, wrapper,
                        wayCount, outLayerMetas));
        ASSERT_EQ(wayCount, outLayerMetas.size());
        auto outLayerMetas1 = outLayerMetas[0];
        auto outLayerMetas2 = outLayerMetas[1];
        ASSERT_EQ(1u, outLayerMetas1->size());
        ASSERT_EQ(DocIdRangeMeta(0, 79), (*outLayerMetas1)[0][0]);
        ASSERT_EQ(DocIdRangeMeta(50, 129), (*outLayerMetas1)[0][1]);
        ASSERT_EQ(1u, outLayerMetas2->size());
        ASSERT_EQ(DocIdRangeMeta(130, 189), (*outLayerMetas2)[0][0]);
        ASSERT_EQ(DocIdRangeMeta(100, 199), (*outLayerMetas2)[0][1]);
    }
    {
        //seg1:0~99, seg2:100~199, seg3:200~249 realtime
        FakeIndex fakeIndex;
        fakeIndex.segmentDocCounts = "100,100|50";
        IndexPartitionReaderWrapperPtr wrapper =
            FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
        LayerMetas inputLayerMetas(_pool);
        LayerMeta lyrMeta1(_pool), lyrMeta2(_pool);
        lyrMeta1.quota = 100;
        lyrMeta2.quota = 200;
        lyrMeta1.push_back(DocIdRangeMeta(0, 59));
        lyrMeta1.push_back(DocIdRangeMeta(100, 149));
        lyrMeta2.push_back(DocIdRangeMeta(150, 179));
        lyrMeta2.push_back(DocIdRangeMeta(200, 249));
        inputLayerMetas.push_back(lyrMeta1);
        inputLayerMetas.push_back(lyrMeta2);
        size_t wayCount = 4;
        vector<LayerMetasPtr> outLayerMetas;
        ASSERT_TRUE(LayerMetaUtil::splitLayerMetas(_pool, inputLayerMetas, wrapper,
                        wayCount, outLayerMetas));
        ASSERT_EQ(wayCount, outLayerMetas.size());
        auto outLayerMetas1 = outLayerMetas[0];
        auto outLayerMetas2 = outLayerMetas[1];
        auto outLayerMetas3 = outLayerMetas[2];
        auto outLayerMetas4 = outLayerMetas[3];
        ASSERT_EQ(2u, outLayerMetas1->size());
        ASSERT_EQ(DocIdRangeMeta(0, 28), (*outLayerMetas1)[0][0]);
        ASSERT_EQ(DocIdRangeMeta(150, 158), (*outLayerMetas1)[1][0]);
        ASSERT_EQ(2u, outLayerMetas2->size());
        ASSERT_EQ(DocIdRangeMeta(29, 59), (*outLayerMetas2)[0][0]);
        ASSERT_EQ(DocIdRangeMeta(159, 165), (*outLayerMetas2)[1][0]);
        ASSERT_EQ(2u, outLayerMetas3->size());
        ASSERT_EQ(DocIdRangeMeta(100, 122), (*outLayerMetas3)[0][0]);
        ASSERT_EQ(DocIdRangeMeta(166, 172), (*outLayerMetas3)[1][0]);
        ASSERT_EQ(2u, outLayerMetas4->size());
        ASSERT_EQ(DocIdRangeMeta(123, 149), (*outLayerMetas4)[0][0]);
        ASSERT_EQ(DocIdRangeMeta(173, 179), (*outLayerMetas4)[1][0]);
        ASSERT_EQ(DocIdRangeMeta(200, 249), (*outLayerMetas4)[1][1]);
    }
    {
        //seg1:0~99, seg2:100~199, seg3:200~249 realtime
        FakeIndex fakeIndex;
        fakeIndex.segmentDocCounts = "100,100|50";
        IndexPartitionReaderWrapperPtr wrapper =
            FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
        LayerMetas inputLayerMetas(_pool);
        LayerMeta lyrMeta1(_pool);
        lyrMeta1.quota = 100;
        inputLayerMetas.push_back(lyrMeta1);
        size_t wayCount = 4;
        vector<LayerMetasPtr> outLayerMetas;
        ASSERT_TRUE(LayerMetaUtil::splitLayerMetas(_pool, inputLayerMetas, wrapper,
                        wayCount, outLayerMetas));
        auto outLayerMetas1 = outLayerMetas[0];
        auto outLayerMetas2 = outLayerMetas[1];
        auto outLayerMetas3 = outLayerMetas[2];
        auto outLayerMetas4 = outLayerMetas[3];
        ASSERT_EQ(1u, outLayerMetas1->size());
        ASSERT_EQ(0u, (*outLayerMetas1)[0].size());
        ASSERT_EQ(1u, outLayerMetas2->size());
        ASSERT_EQ(0u, (*outLayerMetas2)[0].size());
        ASSERT_EQ(1u, outLayerMetas3->size());
        ASSERT_EQ(0u, (*outLayerMetas3)[0].size());
        ASSERT_EQ(1u, outLayerMetas4->size());
    }
    {
        //seg1:0~99, seg2:100~199, seg3:200~249 realtime
        FakeIndex fakeIndex;
        fakeIndex.segmentDocCounts = "100,100|50";
        IndexPartitionReaderWrapperPtr wrapper =
            FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
        LayerMetas inputLayerMetas(_pool);
        LayerMeta lyrMeta1(_pool);
        lyrMeta1.quota = 100;
        inputLayerMetas.push_back(lyrMeta1);
        size_t wayCount = 0;
        vector<LayerMetasPtr> outLayerMetas;
        ASSERT_FALSE(LayerMetaUtil::splitLayerMetas(_pool, inputLayerMetas, wrapper,
                        wayCount, outLayerMetas));
    }
}

TEST_F(LayerMetaUtilTest, testCreateLayerMetas) {
    {
        string layerStr = "quota:3000;"
                          "quota:4500";
        autil::mem_pool::Pool pool;
        QueryLayerClause *layerClause = initQueryLayerClause(layerStr);
        FakeIndex fakeIndex;
        fakeIndex.segmentDocCounts = "10,20";
        IndexPartitionReaderWrapperPtr wrapper =
            FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
        LayerMetasPtr layerMetas = LayerMetaUtil::createLayerMetas(layerClause,
                NULL, wrapper.get(), &pool);
        ASSERT_EQ(2u, layerMetas->size());
        ASSERT_EQ("(quota: 3000 maxQuota: 4294967295 quotaMode: 0 needAggregate: 1 quotaType: 0) begin: 0 end: 9 nextBegin: 0 quota: 0;begin: 10 end: 29 nextBegin: 10 quota: 0;", (*layerMetas)[0].toString());
        ASSERT_EQ("(quota: 4500 maxQuota: 4294967295 quotaMode: 0 needAggregate: 1 quotaType: 0) begin: 0 end: 9 nextBegin: 0 quota: 0;begin: 10 end: 29 nextBegin: 10 quota: 0;", (*layerMetas)[1].toString());
        DELETE_AND_SET_NULL(layerClause);
    }
    {
        autil::mem_pool::Pool pool;
        RankClause *rankClause = new RankClause();
        RankHint *rankHint = new RankHint();
        rankHint->sortField = "dimen2";
        rankClause->setRankHint(rankHint);
        FakeIndex fakeIndex;
        fakeIndex.segmentDocCounts = "6";
        fakeIndex.partitionMeta = "+dimen0,+dimen2";
        fakeIndex.attributes = "dimen0 : int64_t : 1,1,2,2,3,3;"
                                "dimen2 : double  : 2.2,32.3,2.2,2.2,12.0,32.3;";
        string layerStr = "range:dimen0{1,2}*dimen2{[2.2,3.5]}";
        QueryLayerClause *layerClause = initQueryLayerClause(layerStr);
        IndexPartitionReaderWrapperPtr wrapper =
            FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
        LayerMetasPtr layerMetas = LayerMetaUtil::createLayerMetas(layerClause,
                rankClause, wrapper.get(), &pool);
        ASSERT_EQ("(quota: 0 maxQuota: 4294967295 quotaMode: 0 needAggregate: 1 quotaType: 0) begin: 0 end: 0 nextBegin: 0 quota: 0;begin: 2 end: 3 nextBegin: 2 quota: 0;", (*layerMetas)[0].toString());
        ASSERT_EQ(1u, layerMetas->size());
        DELETE_AND_SET_NULL(layerClause);
        DELETE_AND_SET_NULL(rankClause);
    }
}

END_HA3_NAMESPACE(search);
