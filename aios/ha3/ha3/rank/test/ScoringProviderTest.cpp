#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <ha3/rank/ScoringProvider.h>
#include <suez/turing/expression/util/IndexInfos.h>
#include <ha3/config/LegacyIndexInfoHelper.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/MatchDataManager.h>
#include <ha3/search/test/FakeQueryExecutor.h>
#include <ha3/rank/test/TrivialScorer.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3/search/test/FakeAttributeExpressionCreator.h>
#include <ha3/config/LegacyIndexInfoHelper.h>
#include <ha3/index/SectionReaderWrapper.h>
#include <ha3_sdk/testlib/index/FakeIndexReader.h>
#include <indexlib/index/normal/attribute/accessor/section_attribute_reader_impl.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/search/RankResource.h>

using namespace std;
using namespace autil;
using namespace suez::turing;

IE_NAMESPACE_USE(partition);

USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(rank);

class ScoringProviderTest : public TESTBASE {
public:
    ScoringProviderTest();
    ~ScoringProviderTest();
public:
    void setUp();
    void tearDown();
    void setUpAttributeReaderManager();
protected:
    IndexInfos* createIndexInfos();

    template <typename T>
    void testDeclareVariable(const std::string& fieldName);
protected:
    ScoringProvider *_provider;
    AttributeExpressionCreator *_attrExprCreator;
    matchdoc::MatchDoc _matchDoc;
    common::Ha3MatchDocAllocatorPtr _allocator;
    search::MatchDataManager *_manager;
    IndexInfos *_indexInfos;
    config::LegacyIndexInfoHelper *_indexInfoHelper;
    common::DataProvider *_dataProvider;
    autil::mem_pool::Pool *_pool;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _snapshotPtr;
    IE_NAMESPACE(partition)::TableMem2IdMap _emptyTableMem2IdMap;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, ScoringProviderTest);


ScoringProviderTest::ScoringProviderTest() {
    _provider = NULL;
    _matchDoc = matchdoc::INVALID_MATCHDOC;
    _manager = NULL;
    _attrExprCreator = NULL;
    _dataProvider = NULL;
    _pool = new mem_pool::Pool(1024);
}

ScoringProviderTest::~ScoringProviderTest() {
    _allocator.reset();
    delete _pool;
}

void ScoringProviderTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    FakeIndex fakeIndex;
    fakeIndex.attributes = "uid:int32_t:1,2";
    IndexPartitionReaderWrapperPtr wrapperPtr =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _allocator.reset(new common::Ha3MatchDocAllocator(_pool));
    _attrExprCreator = new FakeAttributeExpressionCreator(_pool, wrapperPtr,
            NULL, NULL, NULL, NULL, NULL);
    _matchDoc = matchdoc::INVALID_MATCHDOC;
    _manager = new MatchDataManager;
    _manager->beginLayer();
    _manager->addQueryExecutor(NULL);
    _indexInfos = createIndexInfos();
    _indexInfoHelper = new LegacyIndexInfoHelper();
    _indexInfoHelper->setIndexInfos(_indexInfos);
    _dataProvider = new DataProvider();

    _snapshotPtr.reset(new PartitionReaderSnapshot(&_emptyTableMem2IdMap,
                    &_emptyTableMem2IdMap, &_emptyTableMem2IdMap,
                    std::vector<IndexPartitionReaderInfo>()));
    RankResource rankResource;
    rankResource.pool = _pool;
    rankResource.attrExprCreator = _attrExprCreator;
    rankResource.indexInfoHelper = _indexInfoHelper;
    rankResource.dataProvider = _dataProvider;
    rankResource.matchDocAllocator = _allocator;
    rankResource.matchDataManager = _manager;
    rankResource.partitionReaderSnapshot = _snapshotPtr.get();
    _provider = new ScoringProvider(rankResource);
}

void ScoringProviderTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");

    if (matchdoc::INVALID_MATCHDOC != _matchDoc) {
        _allocator->deallocate(_matchDoc);
        _matchDoc = matchdoc::INVALID_MATCHDOC;
    }

    DELETE_AND_SET_NULL(_provider);
    DELETE_AND_SET_NULL(_attrExprCreator);
    DELETE_AND_SET_NULL(_manager);
    DELETE_AND_SET_NULL(_indexInfoHelper);
    DELETE_AND_SET_NULL(_indexInfos);
    DELETE_AND_SET_NULL(_dataProvider);
    _snapshotPtr.reset();
}

TEST_F(ScoringProviderTest, testRequireMatchData) {
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_TRUE(_provider->requireMatchData());
}


TEST_F(ScoringProviderTest, testDeclareVariable) {
    testDeclareVariable<int32_t>("price");
    tearDown();
    setUp();
    testDeclareVariable<int16_t>("price1");
}

template <typename T>
void ScoringProviderTest::testDeclareVariable(const std::string& fieldName) {
    HA3_LOG(DEBUG, "Begin Test!");
    matchdoc::Reference<T> *ref
        = _provider->declareVariable<T>(fieldName);
    assert(ref);
    _matchDoc = _allocator->allocate((docid_t)1);
    ref->set(_matchDoc, 100);
    T val = ref->get(_matchDoc);
    ASSERT_EQ(T(100), val);
}

TEST_F(ScoringProviderTest, testDeclareVariableTwoTimes) {
    HA3_LOG(DEBUG, "Begin Test!");
    matchdoc::Reference<int32_t> *ref
        = _provider->declareVariable<int32_t>("price");
    ASSERT_TRUE(ref);
    matchdoc::Reference<int32_t> *ref2
        = _provider->declareVariable<int32_t>("price");
    ASSERT_TRUE(ref2);
    ASSERT_EQ(ref, ref2);
}

TEST_F(ScoringProviderTest, testRequireAttribute) {
    HA3_LOG(DEBUG, "Begin Test!");
    const matchdoc::Reference<int32_t> *ref = _provider->requireAttribute<int32_t>("uid");
    ASSERT_TRUE(ref);
}

TEST_F(ScoringProviderTest, testTryRequireAttribute) {
    HA3_LOG(DEBUG, "Begin Test!");
    const matchdoc::Reference<int32_t> *ref = _provider->requireAttribute<int32_t>("uid");
    ASSERT_TRUE(ref);
}

TEST_F(ScoringProviderTest, testRequireAttributeTwoTimes) {
    HA3_LOG(DEBUG, "Begin Test!");
    const matchdoc::Reference<int32_t> *ref = _provider->requireAttribute<int32_t>("uid");
    ASSERT_TRUE(ref);
    const matchdoc::Reference<int32_t> *ref2 = _provider->requireAttribute<int32_t>("uid");
    ASSERT_TRUE(ref2);
    ASSERT_EQ(ref, ref2);
}

TEST_F(ScoringProviderTest, testRequireAttributeWithWrongType) {
    HA3_LOG(DEBUG, "Begin Test!");
    const matchdoc::Reference<float> *ref = _provider->requireAttribute<float>("uid");
    ASSERT_TRUE(!ref);
}

TEST_F(ScoringProviderTest, testTryRequireAttributeWithWrongType) {
    HA3_LOG(DEBUG, "Begin Test!");
    const matchdoc::Reference<float> *ref = _provider->requireAttribute<float>("uid");
    ASSERT_TRUE(!ref);
}

TEST_F(ScoringProviderTest, testRequireAttributeWithWrongAttribute) {
    HA3_LOG(DEBUG, "Begin Test!");
    const matchdoc::Reference<float> *ref = _provider->requireAttribute<float>("nosuchattri");
    ASSERT_TRUE(!ref);
}

TEST_F(ScoringProviderTest, testRequireAttributeTypeUnmatch) {
    HA3_LOG(DEBUG, "Begin Test!");
    const matchdoc::Reference<int32_t> *ref = _provider->requireAttribute<int32_t>("uid");
    ASSERT_TRUE(ref);
    const matchdoc::Reference<float> *ref2 = _provider->requireAttribute<float>("uid");
    ASSERT_TRUE(!ref2);
}

// move to ScorerWrapper
// TEST_F(ScoringProviderTest, testPrepareMatchData) {
//     HA3_LOG(DEBUG, "Begin Test!");
//     const matchdoc::Reference<int32_t> *ref = _provider->requireAttribute<int32_t>("uid");
//     _provider->beginScore();
//     _matchDoc = _allocator->allocate((docid_t)1);
//     ASSERT_TRUE(ref);
//     _provider->prepareMatchDoc(_matchDoc);
//     int32_t val = 0;
//     val = ref->get(_matchDoc);
//     ASSERT_EQ(2, val);
// }

TEST_F(ScoringProviderTest, testPrepareMatchDocWithMatchData) {
    HA3_LOG(DEBUG, "Begin Test!");
    const matchdoc::Reference<MatchData> *matchDataRef
        = _provider->requireMatchData();
    ASSERT_TRUE(matchDataRef);

    auto matchDataRefInAllocator = _allocator->findReference<MatchData>(MATCH_DATA_REF);
    ASSERT_TRUE(matchDataRefInAllocator);
    ASSERT_TRUE(matchDataRefInAllocator == matchDataRef);

    const matchdoc::Reference<int32_t> *ref = _provider->requireAttribute<int32_t>("uid");
    // _provider->beginScore();
    _matchDoc = _allocator->allocate((docid_t)1);
    ASSERT_TRUE(ref);

    // _provider->prepareMatchDoc(_matchDoc);

// TODO:
//    ASSERT_EQ((int32_t)10, tmd.getDocFreq());
}

TEST_F(ScoringProviderTest, testTrivialScorer) {
    HA3_LOG(DEBUG, "Begin Test!");
    TrivialScorer score;
    ASSERT_TRUE(score.beginRequest(_provider));
    score.endRequest();
}

TEST_F(ScoringProviderTest, testIndexInfoHelper) {
    HA3_LOG(DEBUG, "Begin Test!");

    ASSERT_TRUE(_provider);
    const IndexInfoHelper* infoHelper = _provider->getIndexInfoHelper();
    ASSERT_TRUE(infoHelper);

    ASSERT_EQ((indexid_t)0, infoHelper->getIndexId("default"));
    ASSERT_EQ((indexid_t)INVALID_INDEXID,
                         infoHelper->getIndexId("errorIndexName"));

    ASSERT_EQ(0, infoHelper->getFieldPosition("default", "title"));
    ASSERT_EQ(1, infoHelper->getFieldPosition("default", "body"));
    ASSERT_EQ(2, infoHelper->getFieldPosition("default", "description"));
    ASSERT_EQ(-1, infoHelper->getFieldPosition("errorIndexName", "description"));
    ASSERT_EQ(-1, infoHelper->getFieldPosition("default", "errorFieldName"));

    vector<std::string> fields = infoHelper->getFieldList("default");
    ASSERT_EQ((size_t)3, fields.size());

    fields = infoHelper->getFieldList("errorIndexName");
    ASSERT_EQ((size_t)0, fields.size());
}

TEST_F(ScoringProviderTest, testGetSectionReader) {
    HA3_LOG(DEBUG, "Begin Test!");

    ASSERT_TRUE(_provider);

    SectionAttributeReaderPtr sectionReaderPtr(new SectionAttributeReaderImpl);
    FakeIndexReader *fakeIndexReader = new FakeIndexReader;
    fakeIndexReader->addSectionReader("indexName1", sectionReaderPtr);
    IndexReaderPtr indexReaderPtr(fakeIndexReader);
    _provider->setIndexReader(indexReaderPtr);

    SectionReaderWrapperPtr checkWrapperPtr = _provider->getSectionReader("indexName1");
    ASSERT_TRUE(checkWrapperPtr);

    //checkWrapperPtr = _provider->getSectionReader("noSuchIndexName");
    //ASSERT_TRUE(!checkWrapperPtr);
}

IndexInfos* ScoringProviderTest::createIndexInfos() {
    IndexInfos *indexInfos = new IndexInfos();

    IndexInfo *indexInfo = new IndexInfo();
    indexInfo->indexId = 0;
    indexInfo->indexName = "default";
    indexInfo->addField("title", 100);
    indexInfo->addField("body", 50);
    indexInfo->addField("description", 20);
    indexInfos->addIndexInfo(indexInfo);

    return indexInfos;
}
TEST_F(ScoringProviderTest, testInitTracerRefer) {
    ConfigClause *configClause = new ConfigClause();
    configClause->setRankTrace("DEBUG");

    Request *request = new Request();
    unique_ptr<Request> requestPtr(request);
    request->setConfigClause(configClause);

    common::Ha3MatchDocAllocator *allocator = new common::Ha3MatchDocAllocator(_pool);
    unique_ptr<common::Ha3MatchDocAllocator> allocatorPtr(allocator);
    DataProvider dataProvider;

    RankResource rankResource;
    rankResource.pool = _pool;
    rankResource.request = request;
    rankResource.dataProvider = &dataProvider;
    rankResource.matchDocAllocator = _allocator;

    ScoringProvider scoringProvider(rankResource);

    ASSERT_TRUE(scoringProvider.getTracerRefer());
    ASSERT_EQ(ISEARCH_TRACE_DEBUG, scoringProvider.getTracerLevel());
}

TEST_F(ScoringProviderTest, testDeclareGlobalVariable) {
    HA3_LOG(DEBUG, "Begin Test!");

    ASSERT_TRUE(_provider);

    ASSERT_TRUE(!_provider->declareGlobalVariable<std::set<int> >("aaa", true));
    ASSERT_TRUE(_provider->declareGlobalVariable<uint8_t>("aaa", true));
    ASSERT_TRUE(!_provider->declareGlobalVariable<double>("aaa", false));
    ASSERT_TRUE(_provider->declareGlobalVariable<uint8_t>("aaa", false));

    set<int> *st = _provider->declareGlobalVariable<std::set<int> >("set", false);
    ASSERT_TRUE(st);
    st->insert(1);
    st->insert(2);
    st->insert(2);

    set<int> *st2 = _provider->declareGlobalVariable<std::set<int> >("set", false);
    ASSERT_TRUE(st2);
    ASSERT_EQ((size_t)2, st2->size());
    ASSERT_EQ((size_t)1, st2->count(1));
    ASSERT_EQ((size_t)1, st2->count(2));
}

TEST_F(ScoringProviderTest, testScorerSetErrorMessage) {
    HA3_LOG(DEBUG, "Begin Test!");

    ASSERT_TRUE(_provider);

    _provider->setErrorMessage("error: 123");
    ASSERT_EQ(string("error: 123"), _provider->getErrorMessage());
}

END_HA3_NAMESPACE(rank);
