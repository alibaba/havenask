#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/rank/CavaScorerAdapter.h>
#include <suez/turing/common/CavaModuleCache.h>
#include <suez/turing/common/CavaJitWrapper.h>
#include <ha3/search/test/FakeAttributeExpressionCreator.h>
#include <suez/turing/expression/cava/common/CavaPluginManager.h>
#include <suez/turing/util/SessionResourceBuilder.h>
#include <suez/turing/expression/util/ScoringProviderBuilder.h>
#include <ha3/rank/ScoringProvider.h>
#include <ha3/common/Request.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/test/FakeQueryExecutor.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/cava/ScorerProvider.h>

using namespace std;
using namespace suez::turing;
using namespace testing;
using namespace matchdoc;
using namespace tensorflow;
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(rank);

class CavaScorerAdapterTest : public TESTBASE {
public:
    CavaScorerAdapterTest() {}
public:
    void setUp();
    void tearDown();

private:
    void prepareResource();
    void testFileScoreCommon(
        const string &scorerName,
        CavaScorerAdapterPtr &cavaScorerAdapter);
    void testCodeScoreCommon(
        const string &cavaCode,
        const string &scorerName,
        CavaScorerAdapterPtr &cavaScorerAdapter);

private:
    bool _prepareResourceFlag;
    KeyValueMap _kvPairs;
    SessionResourceBuilder _sessionResourceBuilder;
    SessionResource *_sessionResource;
    QueryResource *_queryResource;

    CavaJitWrapperPtr _cavaJitWrapper;
    CavaPluginManagerPtr _cavaPluginManger;

    Ha3MatchDocAllocatorPtr _allocator;
    ScoringProviderTestBuilder _scoringProviderTestBuilder;
    suez::turing::ScoringProvider *_turingScoringProvider;
    suez::turing::ScorerProvider *_turingScorerProvider;
    suez::turing::AttributeExpressionCreator *_attributeExpressionCreator;

    common::DataProviderPtr _dataProvider;
    common::RequestPtr _request;
    rank::ScoringProviderPtr _ha3ScoringProvider;
    ha3::ScorerProviderPtr _ha3ScorerProvider;
    std::vector<matchdoc::MatchDoc> _matchDocs;

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, CavaScorerAdapterTest);

void CavaScorerAdapterTest::setUp() {
    ::cava::CavaJit::globalInit();
    _prepareResourceFlag = false;
}

void CavaScorerAdapterTest::tearDown() {
}

void CavaScorerAdapterTest::prepareResource() {
    ASSERT_FALSE(_prepareResourceFlag) << "prepareResource can not be called multiple times";
    _prepareResourceFlag = true;

    _sessionResourceBuilder._needBuildIndex = false;
    ASSERT_TRUE(_sessionResourceBuilder.genResource(GET_TEMPLATE_DATA_PATH()));
    _sessionResource = _sessionResourceBuilder.getSessionResource();
    _queryResource = _sessionResourceBuilder.getQueryResource();

    // init _cavaJitWrapper
    suez::turing::CavaConfig cavaConfig;
    cavaConfig._cavaConf =
        GET_TEST_DATA_PATH() + "/CavaScorerAdapterTest/ha3_cava_config.json";
    cavaConfig._sourcePath = GET_TEST_DATA_PATH() + "/CavaScorerAdapterTest/source";
    _cavaJitWrapper.reset(new suez::turing::CavaJitWrapper("", cavaConfig, NULL));
    ASSERT_TRUE(_cavaJitWrapper->init());

    _cavaPluginManger.reset(new CavaPluginManager(_cavaJitWrapper, NULL));
    ASSERT_TRUE(_cavaPluginManger->init({}));
    _sessionResource->cavaPluginManager = _cavaPluginManger;

    _allocator.reset(new Ha3MatchDocAllocator(_queryResource->getPool()));
    _scoringProviderTestBuilder._needCheckIndex = false;
    ASSERT_TRUE(_scoringProviderTestBuilder.init(_sessionResource,
                    _queryResource, &_kvPairs, _allocator));
    ASSERT_TRUE(_scoringProviderTestBuilder.genAttributeExpressionCreator());
    _attributeExpressionCreator = _scoringProviderTestBuilder.getAttributeExpressionCreator();
    ASSERT_NE(nullptr, _attributeExpressionCreator);

    _dataProvider.reset(new DataProvider());
    _request.reset(new common::Request());
    _request->setConfigClause(new ConfigClause());
    RankResource rankResource;
    rankResource.pool = _queryResource->getPool();
    rankResource.attrExprCreator = _attributeExpressionCreator;
    rankResource.dataProvider = _dataProvider.get();
    rankResource.matchDocAllocator = _allocator;
    rankResource.request = _request.get();
    rankResource.requestTracer = _queryResource->getTracer();
    rankResource.cavaAllocator = _queryResource->getCavaAllocator();
    _ha3ScoringProvider.reset(new rank::ScoringProvider(rankResource));
    _ha3ScorerProvider.reset(new ha3::ScorerProvider(_ha3ScoringProvider.get(),
                    *_scoringProviderTestBuilder._cavaJitModules));

    auto idRef = _allocator->declare<int32_t>("id");
    auto catRef = _allocator->declare<uint32_t>("cat");
    _matchDocs = _allocator->batchAllocate(4);
    for (size_t i = 0; i < 4; ++i) {
        idRef->set(_matchDocs[i], i + 1);
        catRef->set(_matchDocs[i], i);
    }
}

void CavaScorerAdapterTest::testFileScoreCommon(
        const string &scorerName,
        CavaScorerAdapterPtr &cavaScorerAdapter)
{
    ASSERT_NO_FATAL_FAILURE(prepareResource());
    _request->_configClause->addKVPair("scorer_class_name", scorerName);

    KeyValueMap scorerParameters;
    cavaScorerAdapter.reset(new CavaScorerAdapter(
            scorerParameters,
            _sessionResource->cavaPluginManager, NULL));
    ASSERT_TRUE(cavaScorerAdapter->beginRequest(_ha3ScoringProvider.get()));
    auto tracer = cavaScorerAdapter->_tracer;
    ASSERT_TRUE(tracer->getTraceInfoVec().empty()) << tracer->getTraceInfo();
    ASSERT_TRUE(cavaScorerAdapter->_beginRequestSuccessFlag);
    ASSERT_TRUE(cavaScorerAdapter->_scorerObj);
    _allocator->extend();
}

TEST_F(CavaScorerAdapterTest, testClone) {
    rank::CavaScorerAdapterPtr cavaScorerAdapter;
    ASSERT_NO_FATAL_FAILURE(testFileScoreCommon("SuezTuringScorer", cavaScorerAdapter));
    ASSERT_NE(nullptr, cavaScorerAdapter);

    Scorer *cloneScorer = cavaScorerAdapter->clone();
    ASSERT_NE(nullptr, cloneScorer);
    auto *cloneCavaScorerAdapter = dynamic_cast<CavaScorerAdapter *>(cloneScorer);
    ASSERT_NE(nullptr, cloneCavaScorerAdapter);
    cloneCavaScorerAdapter->destroy();
}

TEST_F(CavaScorerAdapterTest, testSuezTuringFileScoreSuccess) {
    rank::CavaScorerAdapterPtr cavaScorerAdapter;
    ASSERT_NO_FATAL_FAILURE(testFileScoreCommon("SuezTuringScorer", cavaScorerAdapter));
    ASSERT_NE(nullptr, cavaScorerAdapter);

    double scoreValue;
    auto tracer = cavaScorerAdapter->_tracer;
    {
        scoreValue = cavaScorerAdapter->score(_matchDocs[0]);
        ASSERT_TRUE(tracer->getTraceInfoVec().empty()) << tracer->getTraceInfo();
        ASSERT_TRUE(cavaScorerAdapter->_scoreSuccessFlag);
        ASSERT_EQ(0, scoreValue);
    }
    {
        scoreValue = cavaScorerAdapter->score(_matchDocs[1]);
        ASSERT_TRUE(tracer->getTraceInfoVec().empty()) << tracer->getTraceInfo();
        ASSERT_TRUE(cavaScorerAdapter->_scoreSuccessFlag);
        ASSERT_EQ(2, scoreValue);
    }
    {
        scoreValue = cavaScorerAdapter->score(_matchDocs[2]);
        ASSERT_TRUE(tracer->getTraceInfoVec().empty()) << tracer->getTraceInfo();
        ASSERT_TRUE(cavaScorerAdapter->_scoreSuccessFlag);
        ASSERT_EQ(6, scoreValue);
    }
    {
        scoreValue = cavaScorerAdapter->score(_matchDocs[3]);
        ASSERT_TRUE(tracer->getTraceInfoVec().empty()) << tracer->getTraceInfo();
        ASSERT_TRUE(cavaScorerAdapter->_scoreSuccessFlag);
        ASSERT_EQ(12, scoreValue);
    }
}

TEST_F(CavaScorerAdapterTest, testHa3FileBatchScoreSuccess) {
    rank::CavaScorerAdapterPtr cavaScorerAdapter;
    ASSERT_NO_FATAL_FAILURE(testFileScoreCommon("Ha3Scorer", cavaScorerAdapter));
    ASSERT_NE(nullptr, cavaScorerAdapter);

    matchdoc::Reference<score_t> *scoreRef =
        _allocator->declareWithConstructFlagDefaultGroup<score_t>("scoreValue", true);
    _allocator->extend();

    const size_t n = 2;
    auto tracer = cavaScorerAdapter->_tracer;
    for (size_t i = 0; i < _matchDocs.size(); i += n) {
        ScorerParam scorerParam;
        scorerParam.matchDocs = &_matchDocs[i];
        scorerParam.scoreDocCount = std::min<size_t>(_matchDocs.size() - i, n);
        scorerParam.reference = scoreRef;
        cavaScorerAdapter->batchScore(scorerParam);

        ASSERT_TRUE(tracer->getTraceInfoVec().empty()) << tracer->getTraceInfo();
        ASSERT_TRUE(cavaScorerAdapter->_scoreSuccessFlag);
    }
    vector<double> expected = {0, 2, 6, 12};
    ASSERT_EQ(expected.size(), _matchDocs.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        ASSERT_EQ(expected[i], scoreRef->get(_matchDocs[i])) << "idx=" << i;
    }
}

TEST_F(CavaScorerAdapterTest, testHa3FileScoreSuccess) {
    rank::CavaScorerAdapterPtr cavaScorerAdapter;
    ASSERT_NO_FATAL_FAILURE(testFileScoreCommon("Ha3Scorer", cavaScorerAdapter));
    ASSERT_NE(nullptr, cavaScorerAdapter);

    double scoreValue;
    auto tracer = cavaScorerAdapter->_tracer;
    {
        scoreValue = cavaScorerAdapter->score(_matchDocs[0]);
        ASSERT_TRUE(tracer->getTraceInfoVec().empty()) << tracer->getTraceInfo();
        ASSERT_TRUE(cavaScorerAdapter->_scoreSuccessFlag);
        ASSERT_EQ(0, scoreValue);
    }
    {
        scoreValue = cavaScorerAdapter->score(_matchDocs[1]);
        ASSERT_TRUE(tracer->getTraceInfoVec().empty()) << tracer->getTraceInfo();
        ASSERT_TRUE(cavaScorerAdapter->_scoreSuccessFlag);
        ASSERT_EQ(2, scoreValue);
    }
    {
        scoreValue = cavaScorerAdapter->score(_matchDocs[2]);
        ASSERT_TRUE(tracer->getTraceInfoVec().empty()) << tracer->getTraceInfo();
        ASSERT_TRUE(cavaScorerAdapter->_scoreSuccessFlag);
        ASSERT_EQ(6, scoreValue);
    }
    {
        scoreValue = cavaScorerAdapter->score(_matchDocs[3]);
        ASSERT_TRUE(tracer->getTraceInfoVec().empty()) << tracer->getTraceInfo();
        ASSERT_TRUE(cavaScorerAdapter->_scoreSuccessFlag);
        ASSERT_EQ(12, scoreValue);
    }
}

void CavaScorerAdapterTest::testCodeScoreCommon(
        const string &cavaCode,
        const string &scorerName,
        CavaScorerAdapterPtr &cavaScorerAdapter)
{
    ASSERT_NO_FATAL_FAILURE(prepareResource());

    _request->_configClause->addKVPair("scorer_class_name", scorerName);
    size_t hashKey = _cavaJitWrapper->calcHashKey({cavaCode});
    auto cavaJitModule = _cavaJitWrapper->compile("temp", {"temp.cava"}, {cavaCode}, hashKey);
    ASSERT_NE(nullptr, cavaJitModule);
    _queryResource->addCavaJitModule(hashKey, cavaJitModule);
    KeyValueMap scorerParameters;
    cavaScorerAdapter.reset(new CavaScorerAdapter(
                    scorerParameters,
                    _sessionResource->cavaPluginManager, NULL));

    ASSERT_TRUE(cavaScorerAdapter->beginRequest(_ha3ScoringProvider.get()));
    auto tracer = cavaScorerAdapter->_tracer;
    ASSERT_TRUE(tracer->getTraceInfoVec().empty()) << tracer->getTraceInfo();
    ASSERT_TRUE(cavaScorerAdapter->_beginRequestSuccessFlag);
    ASSERT_TRUE(cavaScorerAdapter->_scorerObj);
    _allocator->extend();
}

TEST_F(CavaScorerAdapterTest, testSuezTuringScoreWithCodeSuccess)
{
    ifstream t(GET_TEST_DATA_PATH() + "/CavaScorerAdapterTest/source/SuezTuringScorer.cava");
    string cavaCode((std::istreambuf_iterator<char>(t)),
                    std::istreambuf_iterator<char>());

    CavaScorerAdapterPtr cavaScorerAdapter;
    ASSERT_NO_FATAL_FAILURE(testCodeScoreCommon(cavaCode, "SuezTuringScorer", cavaScorerAdapter));
    ASSERT_NE(nullptr, cavaScorerAdapter);

    double scoreValue;
    auto tracer = cavaScorerAdapter->_tracer;
    {
        scoreValue = cavaScorerAdapter->score(_matchDocs[0]);
        ASSERT_TRUE(tracer->getTraceInfoVec().empty()) << tracer->getTraceInfo();
        ASSERT_TRUE(cavaScorerAdapter->_scoreSuccessFlag);
        ASSERT_EQ(0, scoreValue);
    }
    {
        scoreValue = cavaScorerAdapter->score(_matchDocs[1]);
        ASSERT_TRUE(tracer->getTraceInfoVec().empty()) << tracer->getTraceInfo();
        ASSERT_TRUE(cavaScorerAdapter->_scoreSuccessFlag);
        ASSERT_EQ(2, scoreValue);
    }
    {
        scoreValue = cavaScorerAdapter->score(_matchDocs[2]);
        ASSERT_TRUE(tracer->getTraceInfoVec().empty()) << tracer->getTraceInfo();
        ASSERT_TRUE(cavaScorerAdapter->_scoreSuccessFlag);
        ASSERT_EQ(6, scoreValue);
    }
    {
        scoreValue = cavaScorerAdapter->score(_matchDocs[3]);
        ASSERT_TRUE(tracer->getTraceInfoVec().empty()) << tracer->getTraceInfo();
        ASSERT_TRUE(cavaScorerAdapter->_scoreSuccessFlag);
        ASSERT_EQ(12, scoreValue);
    }
}

TEST_F(CavaScorerAdapterTest, testHa3ScoreWithCodeSuccess)
{
    ifstream t(GET_TEST_DATA_PATH() + "/CavaScorerAdapterTest/source/Ha3Scorer.cava");
    string cavaCode((std::istreambuf_iterator<char>(t)),
                    std::istreambuf_iterator<char>());

    CavaScorerAdapterPtr cavaScorerAdapter;
    ASSERT_NO_FATAL_FAILURE(testCodeScoreCommon(cavaCode, "Ha3Scorer", cavaScorerAdapter));
    ASSERT_NE(nullptr, cavaScorerAdapter);

    double scoreValue;
    auto tracer = cavaScorerAdapter->_tracer;
    {
        scoreValue = cavaScorerAdapter->score(_matchDocs[0]);
        ASSERT_TRUE(tracer->getTraceInfoVec().empty()) << tracer->getTraceInfo();
        ASSERT_TRUE(cavaScorerAdapter->_scoreSuccessFlag);
        ASSERT_EQ(0, scoreValue);
    }
    {
        scoreValue = cavaScorerAdapter->score(_matchDocs[1]);
        ASSERT_TRUE(tracer->getTraceInfoVec().empty()) << tracer->getTraceInfo();
        ASSERT_TRUE(cavaScorerAdapter->_scoreSuccessFlag);
        ASSERT_EQ(2, scoreValue);
    }
    {
        scoreValue = cavaScorerAdapter->score(_matchDocs[2]);
        ASSERT_TRUE(tracer->getTraceInfoVec().empty()) << tracer->getTraceInfo();
        ASSERT_TRUE(cavaScorerAdapter->_scoreSuccessFlag);
        ASSERT_EQ(6, scoreValue);
    }
    {
        scoreValue = cavaScorerAdapter->score(_matchDocs[3]);
        ASSERT_TRUE(tracer->getTraceInfoVec().empty()) << tracer->getTraceInfo();
        ASSERT_TRUE(cavaScorerAdapter->_scoreSuccessFlag);
        ASSERT_EQ(12, scoreValue);
    }
}

END_HA3_NAMESPACE();
