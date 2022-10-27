#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/service/QrsChainManagerCreator.h>
#include <ha3/worker/QrsZoneResource.h>
#include <ha3/test/test.h>

USE_HA3_NAMESPACE(qrs);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(worker);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(sorter);

using namespace std;
using namespace suez;
using namespace build_service::analyzer;

BEGIN_HA3_NAMESPACE(service);

class QrsChainManagerCreatorTest : public TESTBASE {
public:
    QrsChainManagerCreatorTest();
    ~QrsChainManagerCreatorTest();
public:
    void setUp();
    void tearDown();
protected:
    config::ConfigAdapterPtr getConfigAdapter(const std::string &configPath);
protected:
    QrsChainManagerCreator *_qrsChainManagerCreator;
    IndexProvider _indexProvider;
    QrsZoneResourcePtr _resource;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(service, QrsChainManagerCreatorTest);


QrsChainManagerCreatorTest::QrsChainManagerCreatorTest() { 
}

QrsChainManagerCreatorTest::~QrsChainManagerCreatorTest() { 
    if ( _qrsChainManagerCreator ) {
        delete _qrsChainManagerCreator;
    }
}

void QrsChainManagerCreatorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");

    string configurationDir = TEST_DATA_CONF_PATH; 
    configurationDir += "/" + string(CONFIGURATIONS_DIRNAME) + "/" 
                        + string(CONFIGURATION_VERSION_PREFIX) + "_1";
    _qrsChainManagerCreator = new QrsChainManagerCreator();
    _resource.reset(new QrsZoneResource(_indexProvider));
    _resource->_searchService.reset(new multi_call::SearchService());
}

void QrsChainManagerCreatorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    
    if ( _qrsChainManagerCreator ) {
        delete _qrsChainManagerCreator;
        _qrsChainManagerCreator = NULL;
    }
}

TEST_F(QrsChainManagerCreatorTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    
    ConfigAdapterPtr serverAdminAdapterPtr = getConfigAdapter("/configurations/configuration_1/");
    _resource->_configAdapter = serverAdminAdapterPtr;
    qrs::QrsChainManagerPtr qrsChainMgrPtr = 
        _qrsChainManagerCreator->createQrsChainMgr(_resource);
    ASSERT_TRUE(qrsChainMgrPtr);
}

TEST_F(QrsChainManagerCreatorTest, testCreateAnalyzerFactoryFail)
{
    string configRoot = TEST_DATA_PATH;
    configRoot += "/wrong_analyzer/";
    QrsChainManagerCreator qrsChainManagerCreator;
    ConfigAdapterPtr configAdapterPtr(new ConfigAdapter(configRoot));
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configAdapterPtr->getConfigPath()));
    AnalyzerFactoryPtr analyzerFactoryPtr = qrsChainManagerCreator.createAnalyzerFactory(resourceReaderPtr);
    ASSERT_TRUE(!analyzerFactoryPtr);
}

TEST_F(QrsChainManagerCreatorTest, testCreateSorterManager) {
    ConfigAdapterPtr configAdapterPtr = getConfigAdapter("/configurations/configuration_1/");
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configAdapterPtr->getConfigPath()));
    ClusterSorterManagersPtr clusterSorterManagersPtr(
            new ClusterSorterManagers());
    ASSERT_TRUE(_qrsChainManagerCreator->createSorterManager(
                    resourceReaderPtr, configAdapterPtr, clusterSorterManagersPtr,
                    configAdapterPtr->getClusterTableInfoMap()));
    
    //assert clusterSorterManagersPtr
    ASSERT_EQ(size_t(6), clusterSorterManagersPtr->size());
    ASSERT_TRUE(clusterSorterManagersPtr->find("offer_gb.default") 
                   != clusterSorterManagersPtr->end());
    ASSERT_TRUE(clusterSorterManagersPtr->find("auction_bucket.default") 
                   != clusterSorterManagersPtr->end());
    ASSERT_TRUE(clusterSorterManagersPtr->find("auction.default") 
                   != clusterSorterManagersPtr->end());
    ASSERT_TRUE(clusterSorterManagersPtr->find("auction_summary_so.default") 
                   != clusterSorterManagersPtr->end());
    
    ClusterSorterNamesPtr clusterSorterNamesPtr(new ClusterSorterNames());
    _qrsChainManagerCreator->getClusterSorterNames(clusterSorterManagersPtr,
            clusterSorterNamesPtr);
    
    set<string> expectedSortNames1;
    set<string> expectedSortNames2;
    set<string> actualSortNames;
    expectedSortNames1.insert("DEFAULT");
    expectedSortNames1.insert("NULL");
    expectedSortNames2.insert("DEFAULT");
    expectedSortNames2.insert("NULL");
    expectedSortNames2.insert("fake_sorter1");
    
    ASSERT_TRUE(expectedSortNames1 == (*clusterSorterNamesPtr)["offer_gb.default"]);
    ASSERT_TRUE(expectedSortNames2 == (*clusterSorterNamesPtr)["auction_bucket.default"]);
    ASSERT_TRUE(expectedSortNames2 == (*clusterSorterNamesPtr)["auction.default"]);
    ASSERT_TRUE(expectedSortNames1 == (*clusterSorterNamesPtr)["auction_summary_so.default"]);
}

TEST_F(QrsChainManagerCreatorTest, testCreateSorterManagerFailed) {
    ConfigAdapterPtr configAdapterPtr = getConfigAdapter("/configurations/configuration_2/0/");
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configAdapterPtr->getConfigPath()));
    ClusterSorterManagersPtr clusterSorterManagersPtr(
            new ClusterSorterManagers());
    ASSERT_TRUE(!_qrsChainManagerCreator->createSorterManager(resourceReaderPtr,
                    configAdapterPtr, clusterSorterManagersPtr,
                    configAdapterPtr->getClusterTableInfoMap()));
}

ConfigAdapterPtr QrsChainManagerCreatorTest::getConfigAdapter(const string &configPath)
{
    string configRoot = TEST_DATA_CONF_PATH + configPath;
    ConfigAdapterPtr configAdapterPtr(new ConfigAdapter(configRoot));
    assert(configAdapterPtr.get());
    return configAdapterPtr;
}

END_HA3_NAMESPACE(service);

