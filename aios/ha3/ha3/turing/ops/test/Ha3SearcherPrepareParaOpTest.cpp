#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include "ha3/turing/ops/test/Ha3OpTestBase.h"
#include <ha3/turing/ops/Ha3SearcherPrepareParaOp.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3_sdk/testlib/index/FakeIndexPartition.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include "ha3/service/SearcherResource.h"
#include <ha3/common/QueryLayerClause.h>
#include <ha3/common/ConfigClause.h>
#include <ha3/queryparser/ClauseParserContext.h>
#include <ha3/turing/variant/LayerMetasVariant.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/variant/SeekResourceVariant.h>
#include <ha3/search/IndexPartitionWrapper.h>
#include <indexlib/config/index_partition_schema.h>
#include <ha3/search/IndexPartitionWrapper.h>
#include <ha3/config/ConfigAdapter.h>
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/config/index_partition_options.h"
#include <indexlib/partition/partition_reader_snapshot.h>
#include "indexlib/partition/index_application.h"
#include <fslib/fslib.h>
#include <ha3/rank/RankProfileManager.h>

using namespace std;
using namespace testing;
using namespace tensorflow;
using namespace ::testing;
using namespace suez::turing;
using namespace build_service::plugin;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(queryparser);

BEGIN_HA3_NAMESPACE(turing);

class Ha3SearcherPrepareParaOpTest : public Ha3OpTestBase {
public:
    Ha3SearcherPrepareParaOpTest() {
        _indexRoot = GET_TEMP_DATA_PATH() + "Ha3SearcherPrepareParaOpTest";
        _searcherSessionResource = nullptr;
    }
    void SetUp() override {
        OpTestBase::SetUp();
        fslib::fs::FileSystem::remove(_indexRoot);
        initSearcherResource();
    }

    void TearDown() override {
        OpTestBase::TearDown();
        releaseResources();
    }
private:
    void initSearcherResource();
    void releaseResources();
    Ha3RequestVariant prepareRequestVariant();
    void prepareOp(Ha3SearcherPrepareParaOp *&outOp);
    void initIndex();
    void initRankProfileManager(
            RankProfileManagerPtr &rankProfileMgrPtr,
            uint32_t rankSize, uint32_t rerankSize);
private:
    string _indexRoot;
    SearcherSessionResource *_searcherSessionResource;
    PartitionStateMachinePtr _psm;
    IndexPartitionWrapperPtr _indexPartWrapper;
    IE_NAMESPACE(partition)::IndexApplicationPtr _indexApp;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _snapshot;
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(ops, Ha3SearcherPrepareParaOpTest);

Ha3RequestVariant Ha3SearcherPrepareParaOpTest::prepareRequestVariant() {
    RequestPtr requestPtr(new Request);
    requestPtr->setConfigClause(new ConfigClause());
    Ha3RequestVariant variant(requestPtr, &_pool);
    return variant;
}

void Ha3SearcherPrepareParaOpTest::prepareOp(Ha3SearcherPrepareParaOp *&outOp) {
    TF_ASSERT_OK(
            NodeDefBuilder("myop", "Ha3SearcherPrepareParaOp")
            .Input(FakeInput(DT_VARIANT))
            .Input(FakeInput(DT_VARIANT))
            .Finalize(node_def()));
    TF_ASSERT_OK(InitOp());
    outOp = dynamic_cast<Ha3SearcherPrepareParaOp *>(kernel_.get());
}

void Ha3SearcherPrepareParaOpTest::initIndex()
{
    IndexPartitionOptions options;
    string field = "pk:uint64:pk;string1:string;long1:uint32;";
    string index = "pk:primarykey64:pk;index1:string:string1;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "long1", "");
    string zoneName = "auction";
    schema->SetSchemaName(zoneName);
    DictionaryConfigPtr dictConfig = schema->AddDictionaryConfig("hf_dict", "A;C");

    // set bitmap
    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("index1");
    assert(indexConfig);
    indexConfig->SetDictConfig(dictConfig);
    indexConfig->SetHighFreqencyTermPostingType(hp_both);
    _psm.reset(new PartitionStateMachine);
    ASSERT_TRUE(_psm->Init(schema, options, _indexRoot));

    string fullDocs0 = "cmd=add,pk=0,string1=A,long1=0;"
                       "cmd=add,pk=1,string1=B,long1=1;"
                       "cmd=add,pk=2,string1=A,long1=2;"
                       "cmd=add,pk=3,string1=A,long1=3;"
                       "cmd=add,pk=4,string1=A,long1=4;"
                       "cmd=add,pk=5,string1=B,long1=5;";
    ASSERT_TRUE(_psm->Transfer(BUILD_FULL, fullDocs0, "", ""));
    string incDocs1 = "cmd=add,pk=6,string1=A,long1=6,ts=6;"
                      "cmd=add,pk=7,string1=B,long1=7,ts=7;"
                      "cmd=add,pk=8,string1=A,long1=8,ts=8;"
                      "cmd=add,pk=9,string1=B,long1=9,ts=9;"
                      "cmd=add,pk=10,string1=A,long1=10,ts=10;";
    ASSERT_TRUE(_psm->Transfer(BUILD_INC_NO_MERGE, incDocs1, "", ""));
    string rtDocs2 = "cmd=add,pk=11,string1=B,long1=11,ts=11;"
                     "cmd=add,pk=12,string1=B,long1=12,ts=12;"
                     "cmd=add,pk=13,string1=A,long1=13,ts=13;";
    ASSERT_TRUE(_psm->Transfer(BUILD_RT, rtDocs2, "", ""));
    IndexPartitionPtr indexPart = _psm->GetIndexPartition();
    _indexApp.reset(new IE_NAMESPACE(partition)::IndexApplication());
    ASSERT_TRUE(_indexApp->Init({ indexPart }, IE_NAMESPACE(partition)::JoinRelationMap()));
    _snapshot = _indexApp->CreateSnapshot();
    ASSERT_TRUE(_snapshot);
    ConfigAdapterPtr configAdapterPtr(new ConfigAdapter(""));
    configAdapterPtr->_clusterConfigMapPtr.reset(new ClusterConfigMap);
    configAdapterPtr->_clusterConfigMapPtr->insert({zoneName, ClusterConfigInfo()});
    _indexPartWrapper = IndexPartitionWrapper::createIndexPartitionWrapper(
            configAdapterPtr, indexPart, zoneName);
    ASSERT_TRUE(_indexPartWrapper != NULL);
}

void Ha3SearcherPrepareParaOpTest::initRankProfileManager(
        RankProfileManagerPtr &rankProfileMgrPtr,
        uint32_t rankSize, uint32_t rerankSize)
{
    PlugInManagerPtr nullPtr;
    rankProfileMgrPtr.reset(new RankProfileManager(nullPtr));
    ScorerInfo scorerInfo;
    scorerInfo.scorerName = "DefaultScorer";
    scorerInfo.rankSize = rankSize;
    RankProfile *rankProfile = new RankProfile(DEFAULT_RANK_PROFILE);
    rankProfile->addScorerInfo(scorerInfo);
    scorerInfo.rankSize = rerankSize;
    rankProfile->addScorerInfo(scorerInfo); //two scorer
    rankProfileMgrPtr->addRankProfile(rankProfile);

    CavaPluginManagerPtr cavaPluginManagerPtr;
    ResourceReaderPtr resourceReader(
            new ResourceReader(""));
    rankProfileMgrPtr->init(resourceReader, cavaPluginManagerPtr, NULL);
}

void Ha3SearcherPrepareParaOpTest::initSearcherResource() {
    initIndex();
    auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(
            getQueryResource());
    ASSERT_TRUE(searcherQueryResource != nullptr);
    searcherQueryResource->setIndexSnapshot(_snapshot);
    searcherQueryResource->setSeekTimeoutTerminator(TimeoutTerminatorPtr(new TimeoutTerminator(100, 0)));

    auto searcherSessionResource =
        dynamic_pointer_cast<SearcherSessionResource>(_sessionResource);
    _searcherSessionResource = searcherSessionResource.get();
    ASSERT_TRUE(_searcherSessionResource != nullptr);
    SearcherResourcePtr searcherResource(new HA3_NS(service)::SearcherResource);
    searcherResource->setPartCount(1);
    searcherResource->setIndexPartitionWrapper(_indexPartWrapper);
    RankProfileManagerPtr rankProfileMgrPtr;
    initRankProfileManager(rankProfileMgrPtr, 100, 10);
    searcherResource->setRankProfileManager(rankProfileMgrPtr);
    _searcherSessionResource->searcherResource = searcherResource;
}

void Ha3SearcherPrepareParaOpTest::releaseResources()
{
    if (_searcherSessionResource) {
        _searcherSessionResource->searcherResource.reset();
    }
    _indexPartWrapper.reset();
    _snapshot.reset();
    _indexApp.reset();
    _psm.reset();
    fslib::fs::FileSystem::remove(_indexRoot);
}

TEST_F(Ha3SearcherPrepareParaOpTest, testSimple) {
    Ha3SearcherPrepareParaOp *searcherPrepareParaOp;
    prepareOp(searcherPrepareParaOp);
    ASSERT_TRUE(searcherPrepareParaOp != NULL);
    Variant requestVariant = prepareRequestVariant();
    AddInput<Variant>(
            TensorShape({}),
            [&requestVariant](int x) -> Variant {
                return requestVariant;
            });
    LayerMetasPtr inputLayerMetas(new LayerMetas(&_pool));
    LayerMeta lyrMeta(&_pool);
    lyrMeta.push_back(DocIdRangeMeta(0, 79));
    inputLayerMetas->push_back(lyrMeta);
    LayerMetasVariant layermetaVariant(inputLayerMetas);
    AddInput<Variant>(
            TensorShape({}),
            [&layermetaVariant](int x) -> Variant {
                return layermetaVariant;
            });
    TF_ASSERT_OK(RunOpKernel());
    auto pOutRequest = GetOutput(0);
    ASSERT_TRUE(pOutRequest != nullptr);
    auto pOutSeekResource = GetOutput(1);
    ASSERT_TRUE(pOutSeekResource != nullptr);
    SeekResourceVariant *skrv = pOutSeekResource->scalar<Variant>()()
                                .get<SeekResourceVariant>();
    ASSERT_TRUE(skrv != nullptr);
    SeekResourcePtr skrp = skrv->getSeekResource();
    ASSERT_TRUE(skrp);
    ASSERT_EQ(inputLayerMetas, skrp->layerMetas);
    ASSERT_TRUE(skrp->commonResource);
    ASSERT_TRUE(skrp->partitionResource);
    ASSERT_TRUE(skrp->runtimeResource);
    inputLayerMetas->clear();
}

END_HA3_NAMESPACE();
