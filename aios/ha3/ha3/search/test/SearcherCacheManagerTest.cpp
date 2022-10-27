#include <unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/SearcherCacheManager.h>
#include <ha3/search/test/MatchDocSearcherCreator.h>
#include <autil/mem_pool/Pool.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/common/Request.h>
#include <ha3/search/test/CachedMatchDocSearcherTest.h>

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(search);

class SearcherCacheManagerTest : public TESTBASE {
public:
    SearcherCacheManagerTest();
    ~SearcherCacheManagerTest();
public:
    void setUp();
    void tearDown();
protected:
    static IndexPartitionReaderWrapperPtr makeFakeIndexPartReader();
protected:
    autil::mem_pool::Pool _pool;
    IndexPartitionReaderWrapperPtr _indexPartReaderPtr;
    MatchDocSearcherCreatorPtr _matchDocSearcherCreatorPtr;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _snapshotPtr;
    IE_NAMESPACE(partition)::TableMem2IdMap _emptyTableMem2IdMap;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, SearcherCacheManagerTest);


SearcherCacheManagerTest::SearcherCacheManagerTest() {
}

SearcherCacheManagerTest::~SearcherCacheManagerTest() {
}

void SearcherCacheManagerTest::setUp() {
    _matchDocSearcherCreatorPtr.reset(new MatchDocSearcherCreator(&_pool));
    _snapshotPtr.reset(new PartitionReaderSnapshot(&_emptyTableMem2IdMap,
                    &_emptyTableMem2IdMap, &_emptyTableMem2IdMap,
                    std::vector<IndexPartitionReaderInfo>()));
}

void SearcherCacheManagerTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(SearcherCacheManagerTest, testBeforeOptimize) {
    CachedMatchDocSearcherTest cTest;
    cTest.setUp();
    RequestPtr requestPtr = cTest.prepareRequest("config=cluster:default,start:5,hit:3,rerank_size:9&&query=phrase:with");
    OptimizerClause *optimizerClause = new OptimizerClause();
    optimizerClause->addOptimizerName("aux_chain");
    optimizerClause->addOptimizerOption("aux_name#BITMAP|select#ALL");
    requestPtr->setOptimizerClause(optimizerClause);
    ConfigClause *configClause = requestPtr->getConfigClause();

    SearcherCacheClause *cacheClause = requestPtr->getSearcherCacheClause();

    SearcherCacheConfig config;
    config.maxSize = 1024;
    _indexPartReaderPtr = makeFakeIndexPartReader();
    // cacheResult is not from truncate
    {
        MatchDocSearcherPtr searcherPtr = _matchDocSearcherCreatorPtr->createSearcher(
                requestPtr.get(), _indexPartReaderPtr, config, _snapshotPtr);
        SearcherCache *searcherCache = searcherPtr->_searcherCacheManager.getSearcherCache();
        SearcherCacheStrategyPtr cacheStrategy = searcherPtr->_searcherCacheManager.getCacheStrategy();
        auto defaultCacheStrategy = dynamic_cast<DefaultSearcherCacheStrategy*>(cacheStrategy.get());
        ASSERT_TRUE(defaultCacheStrategy != NULL);
        defaultCacheStrategy->_attributeExpressionCreator =
            _matchDocSearcherCreatorPtr->_partitionResource->attributeExpressionCreator.get();
        CacheResultPtr cacheResult(new CacheResult);
        cacheResult->setUseTruncateOptimizer(false);
        ASSERT_TRUE(searcherCache->put(cacheClause->getKey(),
                        (uint32_t)100000, cacheStrategy,
                        cacheResult.get(), &_pool));
        ASSERT_TRUE(searcherPtr->_searcherCacheManager.initCacheBeforeOptimize(
                        requestPtr.get(),
                        searcherPtr->_resource.sessionMetricsCollector, NULL,
                        searcherPtr->_processorResource.docCountLimits.requiredTopK));
        ASSERT_TRUE(!configClause->useTruncateOptimizer());
        defaultCacheStrategy->_attributeExpressionCreator = NULL;
    }

    // cacheResult is from truncate
    {
        configClause->setUseTruncateOptimizer(true);
        MatchDocSearcherPtr searcherPtr = _matchDocSearcherCreatorPtr->createSearcher(
                requestPtr.get(), _indexPartReaderPtr, config, _snapshotPtr);
        SearcherCache *searcherCache = searcherPtr->_searcherCacheManager.getSearcherCache();
        SearcherCacheStrategyPtr cacheStrategy = searcherPtr->_searcherCacheManager.getCacheStrategy();
        auto defaultCacheStrategy = dynamic_cast<DefaultSearcherCacheStrategy*>(cacheStrategy.get());
        ASSERT_TRUE(defaultCacheStrategy != NULL);
        defaultCacheStrategy->_attributeExpressionCreator =
            _matchDocSearcherCreatorPtr->_partitionResource->attributeExpressionCreator.get();
        CacheResultPtr cacheResult(new CacheResult);
        cacheResult->setUseTruncateOptimizer(true);
        ASSERT_TRUE(searcherCache->put(cacheClause->getKey(),
                        (uint32_t)100000, cacheStrategy,
                        cacheResult.get(), &_pool));
        ASSERT_TRUE(searcherPtr->_searcherCacheManager.initCacheBeforeOptimize(
                        requestPtr.get(),
                        searcherPtr->_resource.sessionMetricsCollector, NULL,
                        searcherPtr->_processorResource.docCountLimits.requiredTopK));
        ASSERT_TRUE(configClause->useTruncateOptimizer());
        defaultCacheStrategy->_attributeExpressionCreator = NULL;
    }

    // cacheResult is not from truncate, configClause disable truncate
    {
        MatchDocSearcherPtr searcherPtr = _matchDocSearcherCreatorPtr->createSearcher(
                requestPtr.get(), _indexPartReaderPtr, config, _snapshotPtr);
        SearcherCache *searcherCache = searcherPtr->_searcherCacheManager.getSearcherCache();
        SearcherCacheStrategyPtr cacheStrategy = searcherPtr->_searcherCacheManager.getCacheStrategy();
        auto defaultCacheStrategy = dynamic_cast<DefaultSearcherCacheStrategy*>(cacheStrategy.get());
        ASSERT_TRUE(defaultCacheStrategy != NULL);
        defaultCacheStrategy->_attributeExpressionCreator =
            _matchDocSearcherCreatorPtr->_partitionResource->attributeExpressionCreator.get();
        configClause->setUseTruncateOptimizer(false);
        CacheResultPtr cacheResult(new CacheResult);
        cacheResult->setUseTruncateOptimizer(false);
        ASSERT_TRUE(searcherCache->put(cacheClause->getKey(),
                        (uint32_t)100000, cacheStrategy,
                        cacheResult.get(), &_pool));
        ASSERT_TRUE(searcherPtr->_searcherCacheManager.initCacheBeforeOptimize(
                        requestPtr.get(),
                        searcherPtr->_resource.sessionMetricsCollector, NULL,
                        searcherPtr->_processorResource.docCountLimits.requiredTopK));
        ASSERT_TRUE(!configClause->useTruncateOptimizer());
        defaultCacheStrategy->_attributeExpressionCreator = NULL;
    }

    // cacheResult is not from truncate, configClause does not disable truncate
    {
        MatchDocSearcherPtr searcherPtr = _matchDocSearcherCreatorPtr->createSearcher(
                requestPtr.get(), _indexPartReaderPtr, config, _snapshotPtr);
        SearcherCache *searcherCache = searcherPtr->_searcherCacheManager.getSearcherCache();
        SearcherCacheStrategyPtr cacheStrategy = searcherPtr->_searcherCacheManager.getCacheStrategy();
        auto defaultCacheStrategy = dynamic_cast<DefaultSearcherCacheStrategy*>(cacheStrategy.get());
        ASSERT_TRUE(defaultCacheStrategy != NULL);
        defaultCacheStrategy->_attributeExpressionCreator =
            _matchDocSearcherCreatorPtr->_partitionResource->attributeExpressionCreator.get();
        configClause->setUseTruncateOptimizer(false);
        CacheResultPtr cacheResult(new CacheResult);
        cacheResult->setUseTruncateOptimizer(true);
        ASSERT_TRUE(searcherCache->put(cacheClause->getKey(),
                        (uint32_t)100000, cacheStrategy,
                        cacheResult.get(), &_pool));
        ASSERT_TRUE(searcherPtr->_searcherCacheManager.initCacheBeforeOptimize(
                        requestPtr.get(),
                        searcherPtr->_resource.sessionMetricsCollector, NULL,
                        searcherPtr->_processorResource.docCountLimits.requiredTopK));
        ASSERT_TRUE(!configClause->useTruncateOptimizer());
        defaultCacheStrategy->_attributeExpressionCreator = NULL;
    }
}

TEST_F(SearcherCacheManagerTest, testCreateLayerClause) {
    HA3_LOG(DEBUG, "Begin Test!");
    SearcherCacheConfig config;
    config.maxSize = 1024;

    {
        DocIdRangeVector docIdRanges;
        QueryLayerClausePtr layerClausePtr;
        layerClausePtr.reset(SearcherCacheManager::createLayerClause(
                        docIdRanges));
        ASSERT_TRUE(layerClausePtr);
        ASSERT_EQ((size_t)1, layerClausePtr->getLayerCount());
        LayerDescription *layerDescription =
            layerClausePtr->getLayerDescription(0);
        ASSERT_TRUE(layerDescription);
        ASSERT_EQ(QM_PER_LAYER, layerDescription->getQuotaMode());
        ASSERT_EQ((uint32_t)END_DOCID, layerDescription->getQuota());
        ASSERT_EQ((size_t)1, layerDescription->getLayerRangeCount());
        LayerKeyRange *layerKeyRange = layerDescription->getLayerRange(0);
        ASSERT_TRUE(layerKeyRange);
        ASSERT_EQ(string(LAYERKEY_DOCID), layerKeyRange->attrName);
        ASSERT_EQ((size_t)0, layerKeyRange->ranges.size());
    }

    {
        DocIdRangeVector docIdRanges;
        QueryLayerClausePtr layerClausePtr;
        LayerDescription *layerDescription;
        LayerKeyRange *layerKeyRange;

        docIdRanges.push_back(make_pair(1, 1));
        layerClausePtr.reset(SearcherCacheManager::createLayerClause(
                        docIdRanges));
        ASSERT_TRUE(layerClausePtr);
        ASSERT_EQ((size_t)1, layerClausePtr->getLayerCount());
        layerDescription = layerClausePtr->getLayerDescription(0);
        ASSERT_TRUE(layerDescription);
        ASSERT_EQ(QM_PER_LAYER, layerDescription->getQuotaMode());
        ASSERT_EQ((uint32_t)END_DOCID, layerDescription->getQuota());
        ASSERT_EQ((size_t)1, layerDescription->getLayerRangeCount());
        layerKeyRange = layerDescription->getLayerRange(0);
        ASSERT_TRUE(layerKeyRange);
        ASSERT_EQ(string(LAYERKEY_DOCID), layerKeyRange->attrName);
        ASSERT_EQ((size_t)0, layerKeyRange->ranges.size());
    }

    {
        DocIdRangeVector docIdRanges;
        QueryLayerClausePtr layerClausePtr;
        LayerDescription *layerDescription;
        LayerKeyRange *layerKeyRange;

        docIdRanges.push_back(make_pair(1, 2));
        layerClausePtr.reset(SearcherCacheManager::createLayerClause(
                        docIdRanges));
        ASSERT_TRUE(layerClausePtr);
        ASSERT_EQ((size_t)1, layerClausePtr->getLayerCount());
        layerDescription = layerClausePtr->getLayerDescription(0);
        ASSERT_TRUE(layerDescription);
        ASSERT_EQ(QM_PER_LAYER, layerDescription->getQuotaMode());
        ASSERT_EQ((uint32_t)END_DOCID, layerDescription->getQuota());
        ASSERT_EQ((size_t)1, layerDescription->getLayerRangeCount());
        layerKeyRange = layerDescription->getLayerRange(0);
        ASSERT_TRUE(layerKeyRange);
        ASSERT_EQ(string(LAYERKEY_DOCID), layerKeyRange->attrName);
        ASSERT_EQ((size_t)1, layerKeyRange->ranges.size());
        ASSERT_EQ(string("1"), layerKeyRange->ranges[0].from);
        ASSERT_EQ(string("2"), layerKeyRange->ranges[0].to);
    }
}


IndexPartitionReaderWrapperPtr
SearcherCacheManagerTest::makeFakeIndexPartReader()
{
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] =
        "with:0[1];1[1];2[2];3[3];4[4];5[5];6[6];7[1];8[8];9[9];\n";
    fakeIndex.attributes =
        "price:uint64_t:1, 2, 3, 4, 5, 6, 7, 8, 9, 10;"
        "attr_expire_time:uint32_t:3, 10, 11, 4, 5, 6, 7, 8, 9, 12;"
        "attr_sort:uint32_t:110, 111, 113, 114, 115, 116, 117, 118, 119, 112;"
        "attr_sort2:uint32_t:1, 2, 3, 4, 5, 6, 7, 8, 9, 12;";

    return FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
}


END_HA3_NAMESPACE(search);
