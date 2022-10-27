#include <unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/service/SearcherResourceCreator.h>
#include <ha3/test/test.h>
#include <ha3/config/TypeDefine.h>
#include <ha3/config/ConfigAdapter.h>
#include <indexlib/partition/online_partition.h>
#include <indexlib/partition/index_application.h>
#include <ha3/search/SearcherCache.h>
#include <llvm/Support/TargetSelect.h>
#include <suez/turing/expression/cava/common/CavaPluginManager.h>
#include <suez/turing/expression/function/FunctionInterfaceCreator.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>

BEGIN_HA3_NAMESPACE(service);
using namespace std;
using namespace autil::legacy;
using namespace suez::turing;
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(summary);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(func_expression);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);

typedef int (*FakeScoreProtoType)(CavaCtx *, void *, int, int);

class SearcherResourceCreatorTest : public TESTBASE {
public:
    SearcherResourceCreatorTest();
    ~SearcherResourceCreatorTest();
public:
    void setUp();
    void tearDown();
protected:
    ErrorCode createSearcherResource(SearcherResourcePtr &searchResourcePtr);
private:
    suez::turing::CavaJitWrapperPtr _cavaJitWrapper;
    ConfigAdapterPtr _configAdapterPtr;
    suez::turing::BizPtr _biz;
    IE_NAMESPACE(partition)::IndexApplication::IndexPartitionMap _indexPartitionMap;
    IE_NAMESPACE(partition)::IndexApplicationPtr _indexApp;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(service, SearcherResourceCreatorTest);


SearcherResourceCreatorTest::SearcherResourceCreatorTest() {
}

SearcherResourceCreatorTest::~SearcherResourceCreatorTest() {
}

void SearcherResourceCreatorTest::setUp() {
    ::cava::CavaJit::globalInit();
    _biz.reset(new suez::turing::Biz());
    _indexApp.reset(new IE_NAMESPACE(partition)::IndexApplication);

    HA3_LOG(DEBUG, "setUp!");
}

void SearcherResourceCreatorTest::tearDown() {
    _indexPartitionMap.clear();
    HA3_LOG(DEBUG, "tearDown!");
}

ErrorCode SearcherResourceCreatorTest::createSearcherResource(
        SearcherResourcePtr &searchResourcePtr)
{
    string localBaseDir = string(TEST_DATA_CONF_PATH) + "/configurations/configuration_1/";
    string configRoot = string(TEST_DATA_CONF_PATH) + "/configurations/configuration_1/";
    string dataPath1 = string(TEST_DATA_CONF_PATH) +
                      "/runtimedata/auction/auction/generation_1/partition_1/";
    string dataPath2 = string(TEST_DATA_CONF_PATH) +
                       "/runtimedata/company/company/generation_1/partition_1/";
    string clusterName = "auction.default";

    _configAdapterPtr.reset(new ConfigAdapter(configRoot));
    IndexPartitionPtr indexPartPtr1(new OnlinePartition);
    string secondaryDir;
    indexPartPtr1->Open(dataPath1, secondaryDir, IndexPartitionSchemaPtr(), IndexPartitionOptions());
    IndexPartitionPtr indexPartPtr2(new OnlinePartition);
    indexPartPtr2->Open(dataPath2, secondaryDir, IndexPartitionSchemaPtr(), IndexPartitionOptions());
    proto::Range range;
    range.set_from(1);
    range.set_to(1);
    Cluster2IndexPartitionMapPtr cluster2IndexPartitionMapPtr(new Cluster2IndexPartitionMap);
    (*cluster2IndexPartitionMapPtr)["company"] = indexPartPtr2;

    IndexPartitionWrapperPtr indexPartWrapper =
        IndexPartitionWrapper::createIndexPartitionWrapper(_configAdapterPtr,
                indexPartPtr1, clusterName);
    auto itemTableName = indexPartPtr1->GetSchema()->GetSchemaName();
    IE_NAMESPACE(partition)::JoinRelationMap joinMap;
    _indexPartitionMap.insert(make_pair(indexPartPtr1->GetSchema()->GetSchemaName(), indexPartPtr1));
    _indexPartitionMap.insert(make_pair(indexPartPtr2->GetSchema()->GetSchemaName(), indexPartPtr2));
    _indexApp->Init(_indexPartitionMap, joinMap);
    auto tableInfo = TableInfoConfigurator::createFromIndexApp(itemTableName,
            _indexApp);
    //auto tableInfo = indexPartWrapper->createTableInfo();
    _biz->_tableInfo = tableInfo;
    _biz->_functionInterfaceCreator.reset(new FunctionInterfaceCreator());
    FuncConfig fc;
    _biz->_functionInterfaceCreator->init(fc, {});

    // add buildin sorters
    build_service::plugin::ModuleInfo moduleInfo;
    SorterConfig finalSorterConfig;
    finalSorterConfig.modules.push_back(moduleInfo);
    suez::turing::SorterInfo sorterInfo;
    sorterInfo.sorterName = "DEFAULT";
    sorterInfo.className = "DefaultSorter";
    finalSorterConfig.sorterInfos.push_back(sorterInfo);
    sorterInfo.sorterName = "NULL";
    sorterInfo.className = "NullSorter";
    finalSorterConfig.sorterInfos.push_back(sorterInfo);
    _biz->_sorterManager = SorterManager::create("", finalSorterConfig);
    SearcherResourceCreator searcherResourceCreator(_configAdapterPtr, NULL, _biz.get());
    auto ret =  searcherResourceCreator.createSearcherResource(clusterName, range,
            2, 3, indexPartWrapper, searchResourcePtr);
    return ret.code;
}

TEST_F(SearcherResourceCreatorTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");

    SearcherResourcePtr resource;
    ErrorCode errorCode = createSearcherResource(resource);
    ASSERT_EQ(ERROR_NONE, errorCode);
    EXPECT_EQ(3u, resource->_partCount);
    TableInfoPtr tableInfoPtr = resource->getTableInfo();
    ASSERT_TRUE(tableInfoPtr);
    const AttributeInfos *attrInfos = tableInfoPtr->getAttributeInfos();
    ASSERT_TRUE(attrInfos);
    ASSERT_EQ(uint32_t(9), attrInfos->getAttributeCount());
    ASSERT_TRUE(attrInfos->getAttributeInfo("id"));
    ASSERT_TRUE(attrInfos->getAttributeInfo("company_id"));
    ASSERT_TRUE(attrInfos->getAttributeInfo("name"));
    ASSERT_TRUE(attrInfos->getAttributeInfo("homesite"));
    ASSERT_TRUE(attrInfos->getAttributeInfo("certification_level"));

    RankProfileManagerPtr rankProfileMgrPtr = resource->getRankProfileManager();
    ASSERT_TRUE(rankProfileMgrPtr);

    FunctionInterfaceCreatorPtr funcCreatorPtr = resource->getFuncCreator();
    ASSERT_TRUE(funcCreatorPtr);

    AggSamplerConfigInfo aggSamplerInfo = resource->getAggSamplerConfigInfo();
    ASSERT_EQ((uint32_t)10, aggSamplerInfo.getAggThreshold());
    ASSERT_EQ((uint32_t)2, aggSamplerInfo.getSampleStep());
    ASSERT_EQ(false, aggSamplerInfo.getAggBatchMode());

    IndexPartitionWrapperPtr indexPartWrapperPtr = resource->getIndexPartitionWrapper();
    //ASSERT_TRUE(indexPartWrapperPtr.get());
}

TEST_F(SearcherResourceCreatorTest, testCreateSearcherCache) {
    HA3_LOG(DEBUG, "Begin Test!");
    string configRoot = TEST_DATA_CONF_PATH_HA3;
    ConfigAdapterPtr configAdapterPtr(new ConfigAdapter(configRoot));

    SearcherResourceCreator searcherResourceCreator(configAdapterPtr, NULL, _biz.get());
    string clusterName = "auction";
    SearcherCachePtr searcherCachePtr;
    bool ret = searcherResourceCreator.createSearcherCache(clusterName,
                                                           searcherCachePtr);
    ASSERT_TRUE(ret);
    ASSERT_TRUE(searcherCachePtr != NULL);
    SearcherCacheConfig cacheConfig = searcherCachePtr->getConfig();
    ASSERT_EQ((uint32_t)2048, cacheConfig.maxSize);
    ASSERT_EQ((uint32_t)400000, cacheConfig.maxItemNum);
    ASSERT_EQ((uint32_t)2000, cacheConfig.incDocLimit);
    ASSERT_EQ((uint32_t)10, cacheConfig.incDeletionPercent);
    ASSERT_EQ((float)2.0, cacheConfig.latencyLimitInMs);
    ASSERT_EQ((uint32_t)0, cacheConfig.minAllowedCacheDocNum);
    ASSERT_EQ((uint32_t)100000, cacheConfig.maxAllowedCacheDocNum);

    // test not exist cluster
    searcherCachePtr.reset();
    clusterName = "not_exist_cluster.default";
    ret = searcherResourceCreator.createSearcherCache(clusterName,
            searcherCachePtr);
    ASSERT_TRUE(!ret);
    ASSERT_TRUE(searcherCachePtr == NULL);

    // test not has searcher_cache_config section
    searcherCachePtr.reset();
    clusterName = "auction_bucket.default";
    ret = searcherResourceCreator.createSearcherCache(clusterName,
            searcherCachePtr);
    ASSERT_TRUE(ret);
    ASSERT_TRUE(searcherCachePtr == NULL);

    // test has searcher_cache_config section, but no content
    searcherCachePtr.reset();
    clusterName = "auction_summary_so.default";
    ret = searcherResourceCreator.createSearcherCache(clusterName,
            searcherCachePtr);
    ASSERT_TRUE(ret);
    ASSERT_TRUE(searcherCachePtr == NULL);
}

TEST_F(SearcherResourceCreatorTest, testValidateAttributeFieldsForSummaryProfile)
{
    string configRoot = string(TEST_DATA_CONF_PATH) + "/configurations/configuration_1/";
    ConfigAdapterPtr configAdapterPtr(new ConfigAdapter(configRoot));

    ClusterTableInfoManagerPtr clusterTableInfoMgr =
        configAdapterPtr->getClusterTableInfoManager("auction.default");
    TableInfoPtr tableInfoPtr = clusterTableInfoMgr->getClusterTableInfo();

    // 1. not in attribute field
    {
        SummaryProfileConfig spc;
        spc.addRequiredAttributeField("company_address");

        ASSERT_TRUE(!SearcherResourceCreator::validateAttributeFieldsForSummaryProfile(
                                                                                       spc, tableInfoPtr));
    }
    // 2. duplicate with summary schema
    {
        SummaryProfileConfig spc;
        spc.addRequiredAttributeField("company_name");

        ASSERT_TRUE(!SearcherResourceCreator::validateAttributeFieldsForSummaryProfile(
                                                                                       spc, tableInfoPtr));
    }
    // 3. sub attribute
    {
        SummaryProfileConfig spc;
        spc.addRequiredAttributeField("sub_attribute1");
        ASSERT_TRUE(!SearcherResourceCreator::validateAttributeFieldsForSummaryProfile(
                                                                                       spc, tableInfoPtr));
    }
    // normal
    {
        SummaryProfileConfig spc;
        spc.addRequiredAttributeField("postcode");
        spc.addRequiredAttributeField("company_id");

        ASSERT_TRUE(SearcherResourceCreator::validateAttributeFieldsForSummaryProfile(
                                                                                      spc, tableInfoPtr));
    }
}

END_HA3_NAMESPACE(service);
