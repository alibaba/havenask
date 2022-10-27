#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/MatchDocScorers.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/rank/GlobalMatchData.h>
#include <suez/turing/expression/framework/RankAttributeExpression.h>
#include <suez/turing/expression/framework/JoinDocIdConverterCreator.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/config/IndexInfoHelper.h>
#include <ha3/config/LegacyIndexInfoHelper.h>
#include <ha3/rank/test/TrivialScorer.h>
#include <ha3/rank/ScoringProvider.h>
#include <ha3/search/test/FakeAttributeExpression.h>
#include <ha3/search/test/FakeAttributeExpressionFactory.h>
#include <autil/mem_pool/PoolVector.h>
#include <memory>
#include <string>
#include <ha3/search/test/FakeAttributeExpressionCreator.h>

using namespace std;
using namespace autil::mem_pool;
using namespace suez::turing;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(search);

class MatchDocScorersTest : public TESTBASE {
public:
    MatchDocScorersTest();
    ~MatchDocScorersTest();
public:
    void setUp();
    void tearDown();
protected:
    void fillTableInfo(TableInfo &tableInfo);
    void generateMatchDocs(
            autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocs,
            uint32_t num);
    void checkMatchDocs(
            const autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocs,
            const std::vector<docid_t> &docIds, 
            const std::vector<score_t> &docScores,
            const matchdoc::Reference<score_t> *scoreRef);
protected:
    AttributeExpressionCreatorPtr _attrExprCreator;
    common::Ha3MatchDocAllocatorPtr _allocator;
    common::DataProvider *_dataProvider;
    rank::ScoringProvider *_scoringProvider;
    TableInfoPtr _tableInfo;
    config::IndexInfoHelper *_indexInfoHelper;
    autil::mem_pool::Pool _pool;
    MatchDataManager _matchDataManager;
    SearchCommonResource *_searchCommonResource;
    SearchPartitionResource *_searchPartitionResource;
    SearchProcessorResource *_searchProcessorResource;
    common::RequestPtr _request;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _snapshotPtr;
    IE_NAMESPACE(partition)::TableMem2IdMap _emptyTableMem2IdMap;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, MatchDocScorersTest);


MatchDocScorersTest::MatchDocScorersTest()
{ 
    _scoringProvider = NULL;
    _matchDataManager.beginLayer();
}

MatchDocScorersTest::~MatchDocScorersTest() { 
    _allocator.reset();
}

void MatchDocScorersTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _request.reset(new common::Request());
    _request->setConfigClause(new common::ConfigClause());
    _snapshotPtr.reset(new IE_NAMESPACE(partition)::PartitionReaderSnapshot(
                    &_emptyTableMem2IdMap, &_emptyTableMem2IdMap, &_emptyTableMem2IdMap,
                    std::vector<IndexPartitionReaderInfo>()));
    Pool *pool = &_pool;
    FakeAttributeExpressionFactory * fakeFactory =
        POOL_NEW_CLASS(pool, FakeAttributeExpressionFactory,
                       "uid", "1, 2, 3, 4, 5, 6, 7, 8, 9, 10", pool);
    fakeFactory->setInt32Attribute("uid1", "11, 12, 13, 14, 15, 16, 17, 18, 19, 20");
    fakeFactory->setInt32Attribute("uid2", "21, 22, 23, 24, 25, 26, 27, 28, 29, 30");
    fakeFactory->setInt32Attribute("uid3", "31, 32, 33, 34, 35, 36, 37, 38, 39, 40");

    _allocator.reset(new common::Ha3MatchDocAllocator(pool));
    _tableInfo.reset(new TableInfo());
    fillTableInfo(*_tableInfo);
    _indexInfoHelper = new LegacyIndexInfoHelper(_tableInfo->getIndexInfos());
    _dataProvider = new DataProvider();

    _attrExprCreator.reset(new FakeAttributeExpressionCreator(pool, 
                    IndexPartitionReaderWrapperPtr(), NULL, NULL, NULL, NULL, NULL));
    _attrExprCreator->resetAttrExprFactory(fakeFactory);

    RankResource rankResource;
    rankResource.pool = pool;
    rankResource.attrExprCreator = _attrExprCreator.get();
    rankResource.globalMatchData.setDocCount(2);
    rankResource.indexInfoHelper = _indexInfoHelper;
    rankResource.boostTable = &(_tableInfo->getIndexInfos()->getFieldBoostTable());
    rankResource.dataProvider = _dataProvider;
    rankResource.matchDocAllocator = _allocator;
    rankResource.matchDataManager = &_matchDataManager;
    _scoringProvider = new rank::ScoringProvider(rankResource);

    _searchCommonResource = new SearchCommonResource(pool, _tableInfo, NULL,
            NULL, NULL, NULL, suez::turing::CavaPluginManagerPtr(),
            NULL, NULL, std::map<size_t, ::cava::CavaJitModulePtr>());
    _searchCommonResource->matchDocAllocator = _allocator;
    _searchPartitionResource = new SearchPartitionResource(*_searchCommonResource,
            NULL, IndexPartitionReaderWrapperPtr(),
            _snapshotPtr);
    _searchPartitionResource->attributeExpressionCreator = _attrExprCreator;
    SearchRuntimeResource searchRuntimeResource;
    _searchProcessorResource = new SearchProcessorResource(searchRuntimeResource);
    _searchProcessorResource->scoringProvider = _scoringProvider;
}

void MatchDocScorersTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    DELETE_AND_SET_NULL(_scoringProvider);
    DELETE_AND_SET_NULL(_dataProvider);
    _attrExprCreator.reset();
    _tableInfo.reset();
    DELETE_AND_SET_NULL(_indexInfoHelper);
    DELETE_AND_SET_NULL(_searchProcessorResource);
    DELETE_AND_SET_NULL(_searchPartitionResource);
    DELETE_AND_SET_NULL(_searchCommonResource);
    _request.reset();
    _snapshotPtr.reset();
}


TEST_F(MatchDocScorersTest, testAddScorer) {
    MatchDocScorers matchDocScorers(100, _allocator.get(), &_pool);
    std::vector<Scorer*> scorerVec;
    scorerVec.push_back(new TrivialScorer("s1", "uid1", 1.0));
    scorerVec.push_back(new TrivialScorer("s2", "uid2", 1.0));
    scorerVec.push_back(new TrivialScorer("s3", "uid3", 1.0));
    ASSERT_TRUE(matchDocScorers.init(scorerVec, _request.get(), _indexInfoHelper,
                    *_searchCommonResource, *_searchPartitionResource, *_searchProcessorResource));
    //check ScorerWrapper
    ASSERT_EQ((size_t)3, matchDocScorers._scorerWrappers.size());
}

TEST_F(MatchDocScorersTest, testBeginRequestSuccess) {
    MatchDocScorers matchDocScorers(100, _allocator.get(), &_pool);
    std::vector<Scorer*> scorerVec;
    scorerVec.push_back(new TrivialScorer("s1", "uid1", 1.0));
    scorerVec.push_back(new TrivialScorer("s2", "uid2", 1.0));
    scorerVec.push_back(new TrivialScorer("s3", "uid3", 1.0));
    ASSERT_TRUE(matchDocScorers.init(scorerVec, _request.get(), _indexInfoHelper,
                    *_searchCommonResource, *_searchPartitionResource, *_searchProcessorResource));
    ASSERT_TRUE(matchDocScorers.beginRequest());
}

TEST_F(MatchDocScorersTest, testBeginRequestFailed) {
    MatchDocScorers matchDocScorers(100, _allocator.get(), &_pool);
    std::vector<Scorer*> scorerVec;
    scorerVec.push_back(new TrivialScorer("s1", "uid1", 1.0));
    scorerVec.push_back(new TrivialScorer("s2", "non_exist_attribute", 1.0));
    scorerVec.push_back(new TrivialScorer("s3", "uid3", 1.0));
    ASSERT_TRUE(matchDocScorers.init(scorerVec, _request.get(), _indexInfoHelper,
                    *_searchCommonResource, *_searchPartitionResource, *_searchProcessorResource));
    ASSERT_TRUE(!matchDocScorers.beginRequest());
}

TEST_F(MatchDocScorersTest, testScoreMatchDocs) {
    MatchDocScorers matchDocScorers(100, _allocator.get(), &_pool);
    TrivialScorer *scorer1 = new TrivialScorer("s1", "uid1", 1.0);
    TrivialScorer *scorer2 = new TrivialScorer("s2", "uid2", 2.0);
    std::vector<Scorer*> scorerVec;
    scorerVec.push_back(scorer1);
    scorerVec.push_back(scorer2);

    ASSERT_TRUE(matchDocScorers.init(scorerVec, _request.get(), _indexInfoHelper,
                    *_searchCommonResource, *_searchPartitionResource, *_searchProcessorResource));

    TrivialScorerTrigger scorerTrigger1;
    TrivialScorerTrigger scorerTrigger2;
    scorerTrigger1.baseScore = 1;
    scorerTrigger1.scoreStep = 1;
    scorerTrigger1.delMatchDocs.insert(2);
    scorerTrigger1.delMatchDocs.insert(4);
    
    scorerTrigger2.baseScore = 1;
    scorerTrigger2.scoreStep = 2;
    scorerTrigger2.bReverseMatchDocs = true;
    scorerTrigger2.delMatchDocs.insert(6);
    
    scorer1->setScorerTrigger(scorerTrigger1);
    scorer2->setScorerTrigger(scorerTrigger2);

    ASSERT_TRUE(matchDocScorers.beginRequest());
    PoolVector<matchdoc::MatchDoc> matchDocs(&_pool);
    generateMatchDocs(matchDocs, 10);
    ASSERT_EQ((size_t)10, matchDocs.size());
    uint32_t deleteDocCount = matchDocScorers.scoreMatchDocs(matchDocs);

    ASSERT_EQ((size_t)7, matchDocs.size());
    ASSERT_EQ((uint32_t)3, deleteDocCount);
    docid_t docIds[] = {0,1,3,5,7,8,9};
    score_t docScores[] = {1,3,5,7,11,13,15};
    vector<docid_t> expectedDocIds(docIds, docIds + sizeof(docIds)/sizeof(docid_t));
    reverse(expectedDocIds.begin(), expectedDocIds.end());
    vector<score_t> expectedScores(docScores, docScores+ sizeof(docScores)/sizeof(score_t));
    reverse(expectedScores.begin(), expectedScores.end());
    checkMatchDocs(matchDocs, expectedDocIds, expectedScores, 
                   matchDocScorers._scoreRef);
}

void MatchDocScorersTest::fillTableInfo(TableInfo &tableInfo) {
    IndexInfos *indexInfos = new IndexInfos();

    IndexInfo *indexInfo1 = new IndexInfo();
    indexInfo1->indexName = "defaultIndex";
    indexInfo1->addField("title", 1000);
    indexInfo1->addField("body", 200);
    indexInfo1->addField("description", 50);
    indexInfos->addIndexInfo(indexInfo1);

    tableInfo.setIndexInfos(indexInfos);
}

TEST_F(MatchDocScorersTest, testDoScore) {
    MatchDocScorers matchDocScorers(100, _allocator.get(), &_pool);

    std::vector<Scorer*> scorerVec;
    TrivialScorer *scorer = new TrivialScorer("s1", "uid1", 1.0);
    scorerVec.push_back(scorer);
    ASSERT_TRUE(matchDocScorers.init(scorerVec, _request.get(), _indexInfoHelper,
                    *_searchCommonResource, *_searchPartitionResource, *_searchProcessorResource));

    TrivialScorerTrigger scorerTrigger;
    scorerTrigger.baseScore = 1;
    scorerTrigger.scoreStep = 3;
    scorerTrigger.delMatchDocs.insert(0);
    scorerTrigger.delMatchDocs.insert(3);
    scorerTrigger.delMatchDocs.insert(5);
    scorer->setScorerTrigger(scorerTrigger);
    ASSERT_TRUE(matchDocScorers.beginRequest());


    PoolVector<matchdoc::MatchDoc> matchDocs(&_pool);
    generateMatchDocs(matchDocs, 6);
    uint32_t deleteDocNum = matchDocScorers.doScore(0, matchDocs);

    ASSERT_EQ((uint32_t)3, deleteDocNum);
    ASSERT_EQ((size_t)3, matchDocs.size());

    docid_t docIds[] = {1,2,4};
    score_t docScores[] = {4,7,13};
    vector<docid_t> expectedDocIds(docIds, docIds + sizeof(docIds)/sizeof(docid_t));
    vector<score_t> expectedScores(docScores, docScores+ sizeof(docScores)/sizeof(score_t));
    checkMatchDocs(matchDocs, expectedDocIds, expectedScores,
                   matchDocScorers._scoreRef);
}

void MatchDocScorersTest::generateMatchDocs(PoolVector<matchdoc::MatchDoc> &matchDocs,
        uint32_t num)
{
    for (uint32_t i= 0; i < num; i++) {
        matchDocs.push_back(_allocator->allocate(i));
    }
}

void MatchDocScorersTest::checkMatchDocs(
        const PoolVector<matchdoc::MatchDoc> &matchDocs,
        const vector<docid_t> &docIds, 
        const vector<score_t> &docScores,
        const matchdoc::Reference<score_t> *scoreRef) {
    ASSERT_EQ(matchDocs.size(), docIds.size());
    ASSERT_EQ(matchDocs.size(), docScores.size());
    for (uint32_t i = 0; i < matchDocs.size(); i++) {
        ASSERT_EQ(docIds[i], matchDocs[i].getDocId());
        ASSERT_EQ(docScores[i], 
                             scoreRef->getReference(
                                     matchDocs[i]));
    }
}

END_HA3_NAMESPACE(search);

