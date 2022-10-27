#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/test/TrivialScorer.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <suez/turing/expression/plugin/ScorerWrapper.h>
#include <ha3/rank/ScoringProvider.h>
#include <ha3/common/Request.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/test/FakeQueryExecutor.h>
#include <ha3/search/test/FakeAttributeExpressionCreator.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>

using namespace std;
using namespace autil;
using namespace suez::turing;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);

USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(rank);

class TrivialScorerTest : public TESTBASE {
public:
    TrivialScorerTest();
    ~TrivialScorerTest();
public:
    void setUp();
    void tearDown();

protected:
    void setUpAttributeReaderManager();

protected:
    TrivialScorer *_scorer;
    ScoringProvider *_provider;
    suez::turing::AttributeExpressionCreator *_attrExprCreator;
    matchdoc::MatchDoc _matchDoc;
    common::Ha3MatchDocAllocatorPtr _allocator;
    common::DataProvider *_dataProvider;
    ScorerWrapper *_scorerWrapper;
    search::MatchDataManager *_manager;
    autil::mem_pool::Pool *_pool;
    matchdoc::Reference<score_t> *_ref;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _snapshotPtr;
    IE_NAMESPACE(partition)::TableMem2IdMap _emptyTableMem2IdMap;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, TrivialScorerTest);

TrivialScorerTest::TrivialScorerTest() {
    _scorer = NULL;
    _provider = NULL;
    _matchDoc = matchdoc::INVALID_MATCHDOC;
    _attrExprCreator = NULL;
    _manager = NULL;
    _pool = new mem_pool::Pool(1024);
}

TrivialScorerTest::~TrivialScorerTest() {
    _allocator.reset();
    delete _pool;
}

void TrivialScorerTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    FakeIndex fakeIndex;
    fakeIndex.attributes = "uid:int32_t:1,2";
    IndexPartitionReaderWrapperPtr wrapperPtr =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _allocator.reset(new common::Ha3MatchDocAllocator(_pool));
    _attrExprCreator = new FakeAttributeExpressionCreator(_pool, wrapperPtr,
            NULL, NULL, NULL, NULL, NULL);

    _manager = new search::MatchDataManager;;
    _scorer = new TrivialScorer;
    _dataProvider = new DataProvider();
    _ref = _allocator->declare<score_t>(suez::turing::SCORE_REF);
    _scorerWrapper = new ScorerWrapper(_scorer, _ref);

    _snapshotPtr.reset(new PartitionReaderSnapshot(&_emptyTableMem2IdMap,
                    &_emptyTableMem2IdMap, &_emptyTableMem2IdMap,
                    std::vector<IndexPartitionReaderInfo>()));

    RankResource rankResource;
    rankResource.pool = _pool;
    rankResource.attrExprCreator = _attrExprCreator;
    rankResource.dataProvider = _dataProvider;
    rankResource.matchDocAllocator = _allocator;
    rankResource.partitionReaderSnapshot = _snapshotPtr.get();

    _manager->beginLayer();
    rankResource.matchDataManager = _manager;
    _provider = new ScoringProvider(rankResource);
}

void TrivialScorerTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");

    DELETE_AND_SET_NULL(_scorerWrapper);
    if (matchdoc::INVALID_MATCHDOC != _matchDoc) {
        _allocator->deallocate(_matchDoc);
        _matchDoc = matchdoc::INVALID_MATCHDOC;
    }
    DELETE_AND_SET_NULL(_manager);
    DELETE_AND_SET_NULL(_dataProvider);
    DELETE_AND_SET_NULL(_provider);
    DELETE_AND_SET_NULL(_attrExprCreator);
    _snapshotPtr.reset();
}

TEST_F(TrivialScorerTest, testTrivialScorer) {
    HA3_LOG(DEBUG, "Begin Test!");

    ASSERT_TRUE(_scorerWrapper->beginRequest(_provider));
    _matchDoc = _allocator->allocate((docid_t)1);
    ASSERT_DOUBLE_EQ(3.0f, _scorerWrapper->score(_matchDoc));
    _scorerWrapper->endRequest();
}

END_HA3_NAMESPACE(rank);
