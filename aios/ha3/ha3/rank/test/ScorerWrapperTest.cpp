#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/plugin/ScorerWrapper.h>
#include <build_service/plugin/PlugInManager.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/config/ResourceReader.h>
#include <ha3/config/LegacyIndexInfoHelper.h>
#include <suez/turing/expression/framework/RankAttributeExpression.h>
#include <ha3/search/test/FakeAttributeExpressionCreator.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/config/IndexInfoHelper.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/rank/test/TrivialScorer.h>
#include <ha3/search/test/FakeAttributeExpression.h>
#include <ha3/search/test/FakeAttributeExpressionFactory.h>
#include <ha3/search/test/FakeQueryExecutor.h>
#include <memory>
#include <string>

using namespace std;
using namespace suez::turing;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);

USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(rank);

class ScorerWrapperTest : public TESTBASE {
public:
    ScorerWrapperTest();
    ~ScorerWrapperTest();
public:
    void setUp();
    void tearDown();
protected:
    typedef std::tr1::shared_ptr<autil::mem_pool::Pool> PoolPtr;
    PoolPtr _poolPtr;
    suez::turing::AttributeExpressionCreator *_attrExprCreator;
    common::Ha3MatchDocAllocatorPtr _allocator;
    common::DataProvider *_dataProvider;
    rank::ScoringProvider *_scoringProvider;
    suez::turing::TableInfo *_tableInfo;
    config::IndexInfoHelper *_indexInfoHelper;
    matchdoc::Reference<score_t> *_scoreRef;
    search::MatchDataManager _matchDataManager;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _snapshotPtr;
    IE_NAMESPACE(partition)::TableMem2IdMap _emptyTableMem2IdMap;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, ScorerWrapperTest);


ScorerWrapperTest::ScorerWrapperTest() {
    _attrExprCreator = NULL;
    _scoringProvider = NULL;
    _matchDataManager.beginLayer();
    _poolPtr = PoolPtr(new autil::mem_pool::Pool(1024));
}

ScorerWrapperTest::~ScorerWrapperTest() {
}

void ScorerWrapperTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    autil::mem_pool::Pool *pool;
    pool = _poolPtr.get();
    FakeAttributeExpressionFactory * fakeFactory =
        POOL_NEW_CLASS(pool, FakeAttributeExpressionFactory,
                       "uid", "1, 2", pool);
    fakeFactory->setInt32Attribute("uid1", "1, 2");
    fakeFactory->setInt32Attribute("uid2", "3, 4");
    fakeFactory->setInt32Attribute("uid3", "5, 6");

    _allocator.reset(new common::Ha3MatchDocAllocator(pool));
    matchdoc::Reference<score_t> *scoreRef
        = _allocator->declare<score_t>(suez::turing::SCORE_REF);
    _scoreRef = scoreRef;
    _tableInfo = new TableInfo();
    _indexInfoHelper = new LegacyIndexInfoHelper(_tableInfo->getIndexInfos());
    _dataProvider = new DataProvider();

    IndexPartitionReaderWrapperPtr indexPartitionReaderWrapper; // unused
    _attrExprCreator = new FakeAttributeExpressionCreator(pool,
            indexPartitionReaderWrapper, NULL, NULL, NULL, NULL, NULL);
    _attrExprCreator->resetAttrExprFactory(fakeFactory);
    _snapshotPtr.reset(new PartitionReaderSnapshot(&_emptyTableMem2IdMap,
                    &_emptyTableMem2IdMap, &_emptyTableMem2IdMap,
                    std::vector<IndexPartitionReaderInfo>()));

    RankResource rankResource;
    rankResource.pool = pool;
    rankResource.attrExprCreator = _attrExprCreator;
    rankResource.globalMatchData.setDocCount(2);
    rankResource.indexInfoHelper = _indexInfoHelper;
    rankResource.boostTable = &(_tableInfo->getIndexInfos()->getFieldBoostTable());
    rankResource.dataProvider = _dataProvider;
    rankResource.matchDocAllocator = _allocator;
    rankResource.matchDataManager = &_matchDataManager;
    rankResource.partitionReaderSnapshot = _snapshotPtr.get();
    _scoringProvider = new ScoringProvider(rankResource);
}

void ScorerWrapperTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    DELETE_AND_SET_NULL(_scoringProvider);
    DELETE_AND_SET_NULL(_dataProvider);
    DELETE_AND_SET_NULL(_attrExprCreator);
    DELETE_AND_SET_NULL(_tableInfo);
    DELETE_AND_SET_NULL(_indexInfoHelper);
    _allocator.reset();
    _snapshotPtr.reset();
}

TEST_F(ScorerWrapperTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");
    Scorer *scorer1 = new TrivialScorer("s1", "uid1", 1.0);
    Scorer *scorer2 = new TrivialScorer("s2", "non_exist_attribute", 1.0);
    ScorerWrapper scorerWarpper1(scorer1, _scoreRef);
    ScorerWrapper scorerWarpper2(scorer2, _scoreRef);
    ASSERT_TRUE(scorerWarpper1.beginRequest(_scoringProvider));
    ASSERT_TRUE(!scorerWarpper2.beginRequest(_scoringProvider));
    matchdoc::MatchDoc matchDoc = _allocator->allocate(0);
    scorerWarpper1.score(matchDoc);
    ASSERT_EQ((score_t) 2,
                         _scoreRef->getReference(matchDoc));
}

END_HA3_NAMESPACE(rank);
