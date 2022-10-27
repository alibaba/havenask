#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <build_service/plugin/PlugInManager.h>
#include <ha3/qrs/QrsChainManagerConfigurator.h>
#include <ha3/test/test.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(config);
using namespace build_service::plugin;
USE_HA3_NAMESPACE(service);
using namespace build_service::analyzer;
using namespace multi_call;

BEGIN_HA3_NAMESPACE(qrs);

class QrsChainManagerConfiguratorTest : public TESTBASE {
public:
    QrsChainManagerConfiguratorTest();
    ~QrsChainManagerConfiguratorTest();
public:
    void setUp();
    void tearDown();
protected:
    QrsChainManagerConfiguratorPtr _qrsChainManagerConfigurator;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, QrsChainManagerConfiguratorTest);


QrsChainManagerConfiguratorTest::QrsChainManagerConfiguratorTest() {
}

QrsChainManagerConfiguratorTest::~QrsChainManagerConfiguratorTest() {
}

void QrsChainManagerConfiguratorTest::setUp() {

    string schemaConf = string(TEST_DATA_CONF_PATH_HA3) + "/schemas/auction_schema.json";
    TableInfoConfigurator tableInfoConfigurator;
    TableInfoPtr tableInfoPtr = tableInfoConfigurator.createFromFile(schemaConf);
    ASSERT_TRUE(tableInfoPtr);
    ClusterTableInfoMapPtr clusterTableInfoMapPtr(new ClusterTableInfoMap);
    (*clusterTableInfoMapPtr)["auction"] = tableInfoPtr;

    ResourceReaderPtr resourceReaderPtr(new ResourceReader(TEST_DATA_CONF_PATH_HA3));
    AnalyzerFactoryPtr analyzerFactoryPtr;
    ClusterConfigMapPtr clusterConfigMapPtr(new ClusterConfigMap());
    _qrsChainManagerConfigurator.reset(new QrsChainManagerConfigurator(
            clusterTableInfoMapPtr, clusterConfigMapPtr,
            analyzerFactoryPtr, ClusterFuncMapPtr(), CavaPluginManagerMapPtr(),
            resourceReaderPtr,
            ClusterSorterManagersPtr(new ClusterSorterManagers),
            ClusterSorterNamesPtr(new ClusterSorterNames)));
}

void QrsChainManagerConfiguratorTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(QrsChainManagerConfiguratorTest, testDebugQrsChainHasBuildInConfig) {
        string configFile = string(TEST_DATA_CONF_PATH_HA3) + "/qrs_with_debug.json.in";
        QrsChainManagerPtr qrsChainMgrPtr =
            _qrsChainManagerConfigurator->createFromFile(configFile);
        ASSERT_TRUE(qrsChainMgrPtr);
        QrsProcessorPtr qrsProcessorPtr
            = qrsChainMgrPtr->getProcessorChain(DEFAULT_DEBUG_QRS_CHAIN);
        ASSERT_TRUE(qrsProcessorPtr);
        ASSERT_EQ(string("CheckSummaryProcessor"),
                             qrsProcessorPtr->getName());
        qrsProcessorPtr = qrsProcessorPtr->getNextProcessor();
        ASSERT_TRUE(qrsProcessorPtr);

        ASSERT_EQ(string("RequestParserProcessor"),
                             qrsProcessorPtr->getName());
        qrsProcessorPtr = qrsProcessorPtr->getNextProcessor();
        ASSERT_TRUE(qrsProcessorPtr);
        ASSERT_EQ(string("RequestValidateProcessor"),
                             qrsProcessorPtr->getName());
        qrsProcessorPtr = qrsProcessorPtr->getNextProcessor();
        ASSERT_TRUE(qrsProcessorPtr);
        ASSERT_EQ(string(DEFAULT_DEBUG_PROCESSOR),
                             qrsProcessorPtr->getName());
        ASSERT_EQ(string("222"), qrsProcessorPtr->getParam("111"));

        qrsProcessorPtr = qrsProcessorPtr->getNextProcessor();
        ASSERT_TRUE(qrsProcessorPtr);
        ASSERT_EQ(string("QrsSearchProcessor"),
                             qrsProcessorPtr->getName());
        qrsProcessorPtr = qrsProcessorPtr->getNextProcessor();
        ASSERT_TRUE(!qrsProcessorPtr);
}


TEST_F(QrsChainManagerConfiguratorTest, testDebugQrsChain) {
    {
        string configFile = string(TEST_DATA_CONF_PATH_HA3) + "/qrs.json";
        QrsChainManagerPtr qrsChainMgrPtr =
            _qrsChainManagerConfigurator->createFromFile(configFile);
        ASSERT_TRUE(qrsChainMgrPtr);
        QrsProcessorPtr qrsProcessorPtr
            = qrsChainMgrPtr->getProcessorChain(DEFAULT_DEBUG_QRS_CHAIN);
        ASSERT_TRUE(qrsProcessorPtr);
        ASSERT_EQ(string("CheckSummaryProcessor"),
                             qrsProcessorPtr->getName());
        qrsProcessorPtr = qrsProcessorPtr->getNextProcessor();
        ASSERT_TRUE(qrsProcessorPtr);

        ASSERT_EQ(string("RequestParserProcessor"),
                             qrsProcessorPtr->getName());
        qrsProcessorPtr = qrsProcessorPtr->getNextProcessor();
        ASSERT_TRUE(qrsProcessorPtr);
        ASSERT_EQ(string("RequestValidateProcessor"),
                             qrsProcessorPtr->getName());
        qrsProcessorPtr = qrsProcessorPtr->getNextProcessor();
        ASSERT_TRUE(qrsProcessorPtr);
        ASSERT_EQ(string(DEFAULT_DEBUG_PROCESSOR),
                             qrsProcessorPtr->getName());

        qrsProcessorPtr = qrsProcessorPtr->getNextProcessor();
        ASSERT_TRUE(qrsProcessorPtr);
        ASSERT_EQ(string("QrsSearchProcessor"),
                             qrsProcessorPtr->getName());
        qrsProcessorPtr = qrsProcessorPtr->getNextProcessor();
        ASSERT_TRUE(!qrsProcessorPtr);
    }
}

TEST_F(QrsChainManagerConfiguratorTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");
    string basePath = TOP_BUILDDIR;
    string configFile = basePath + "/testdata/conf/qrs_chain.conf";
    QrsChainManagerPtr qrsChainManagerPtr =
        _qrsChainManagerConfigurator->createFromFile(configFile);

    ASSERT_TRUE(qrsChainManagerPtr);
}

TEST_F(QrsChainManagerConfiguratorTest, testQrsRule) {
    HA3_LOG(DEBUG, "Begin Test!");
    string basePath = TOP_BUILDDIR;
    string configFile = basePath + "/testdata/conf/qrs_chain.conf";
    QrsChainManagerPtr qrsChainManagerPtr =
        _qrsChainManagerConfigurator->createFromFile(configFile);

    ASSERT_TRUE(qrsChainManagerPtr);
    ASSERT_EQ(qrsChainManagerPtr->getQRSRule()._retHitsLimit, (uint32_t)2000);
}

TEST_F(QrsChainManagerConfiguratorTest, testCreateFailWithInvalidQrsConfig) {
    ClusterTableInfoMapPtr clusterTableInfoMapPtr;
    QrsChainManagerConfigurator qrsChainManagerConfigurator(
            clusterTableInfoMapPtr, ClusterConfigMapPtr(),
            AnalyzerFactoryPtr(), ClusterFuncMapPtr(), CavaPluginManagerMapPtr(),
            ResourceReaderPtr(),
            ClusterSorterManagersPtr(),
            ClusterSorterNamesPtr());
    QrsChainManagerPtr qrsChainManagerPtr =
        qrsChainManagerConfigurator.createFromString("wrong qrs config");
    ASSERT_TRUE(!qrsChainManagerPtr);
}

END_HA3_NAMESPACE(qrs);
