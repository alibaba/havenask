#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <build_service/plugin/PlugInManager.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/qrs/QrsProcessor.h>
#include <ha3/qrs/QrsChainManager.h>
#include <ha3/config/ClusterConfigInfo.h>
#include <string>
#include <ha3/qrs/QrsChainManager.h>
#include <ha3/qrs/test/FakeRawStringProcessor.h>
#include <ha3/test/test.h>
#include <ha3/qrs/RequestParserProcessor.h>
#include <ha3/qrs/QrsSearchProcessor.h>
#include <ha3/qrs/test/FakeStringProcessor.h>
#include <ha3/qrs/test/FakeRequestProcessor.h>
#include <ha3/qrs/test/FakeLSDSProcessor.h>
#include <ha3/config/QrsConfig.h>
#include <ha3/qrs/QrsChainConfigurator.h>
#include <ha3/qrs/QrsChainManagerConfigurator.h>
#include <ha3/qrs/PageDistinctProcessor.h>
#include <ha3/qrs/RequestValidateProcessor.h>
#include <ha3/qrs/CheckSummaryProcessor.h>
#include <suez/turing/common/FileUtil.h>
#include <ha3/turing/qrs/QrsBiz.h>

using namespace std;
using namespace suez::turing;
using namespace build_service::plugin;
using namespace build_service::analyzer;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(turing);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(service);

BEGIN_HA3_NAMESPACE(qrs);

class QrsChainManagerTest : public TESTBASE {
public:
    QrsChainManagerTest();
    ~QrsChainManagerTest();
public:
    void setUp();
    void tearDown();
protected:
    QrsProcessorPtr makeFakeChain();
    QrsChainManager* makeFakeChainManager();
    typedef std::map<std::string, std::vector<std::string> > ChainProcessorMap;
    QrsChainManager* makeFakeChainManager(
            const ChainProcessorMap &chainProcessors);
    template <typename ProcessorType>
    void checkProcessor(const std::string &processorName,
            QrsProcessorPtr processorPtr);

protected:
    common::RequestPtr prepareEmptyRequest();
    common::ResultPtr prepareEmptyResult();
    build_service::plugin::PlugInManagerPtr _plugInManagerPtr;
    ClusterTableInfoMapPtr _clusterTableInfoMapPtr;
    config::ClusterConfigMapPtr _clusterConfigMapPtr;
    config::ResourceReaderPtr _resourceReaderPtr;
    turing::QrsBizPtr _qrsBiz;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, QrsChainManagerTest);


QrsChainManagerTest::QrsChainManagerTest() {

}

QrsChainManagerTest::~QrsChainManagerTest() {
}

void QrsChainManagerTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    TableInfoPtr tableInfoPtr(new TableInfo("defaultTable"));
    _clusterTableInfoMapPtr.reset(new ClusterTableInfoMap);
    (*_clusterTableInfoMapPtr)["cluster_name"] = tableInfoPtr;

    string basePath = TOP_BUILDDIR;
    string plugConfPath = basePath + "/testdata/conf/qrs_plugin.conf";
    _resourceReaderPtr.reset(new ResourceReader(TEST_DATA_CONF_PATH_HA3));

    _plugInManagerPtr.reset(new PlugInManager(_resourceReaderPtr));
    vector<ModuleInfo> moduleInfos;
    string moduleConfigStr = FileUtil::readFile(plugConfPath);
    autil::legacy::Any any;
    ASSERT_EQ(ResourceReader::JSON_PATH_OK, ResourceReader::getJsonNode(moduleConfigStr, "", any));
    FromJson(moduleInfos, any);
    _plugInManagerPtr->addModules(moduleInfos);
    _clusterConfigMapPtr.reset(new ClusterConfigMap);
}

void QrsChainManagerTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(QrsChainManagerTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");
    string pipelineId = "p1";
    string basePath = TOP_BUILDDIR;
    string configFile = basePath + "/testdata/conf/qrs_chain.conf";
    _qrsBiz.reset(new QrsBiz);
    QrsChainManagerConfigurator qrsChainManagerConfigurator(
            _clusterTableInfoMapPtr,
            _clusterConfigMapPtr, AnalyzerFactoryPtr(), ClusterFuncMapPtr(), CavaPluginManagerMapPtr(),
            _resourceReaderPtr,
            ClusterSorterManagersPtr(new ClusterSorterManagers),
            ClusterSorterNamesPtr(new ClusterSorterNames));
    QrsChainManagerPtr qrsChainManagerPtr =
        qrsChainManagerConfigurator.createFromFile(configFile);

    ASSERT_TRUE(qrsChainManagerPtr);

    QrsProcessorPtr qrsProcessorPtr
        = qrsChainManagerPtr->getProcessorChain("web");
    ASSERT_TRUE(qrsProcessorPtr.get());

//     RequestPtr requestPtr = prepareEmptyRequest();
//     ResultPtr resultPtr = prepareEmptyResult();
//     qrsProcessorPtr->process(requestPtr, resultPtr);
}

TEST_F(QrsChainManagerTest, testGetChain) {
    HA3_LOG(DEBUG, "Begin Test!");

    QrsProcessorPtr qrsProcessorPtr1 = makeFakeChain();
    QrsProcessorPtr qrsProcessorPtr2 = makeFakeChain();
    QrsChainManager qrsChainManager(_plugInManagerPtr, _resourceReaderPtr);
    qrsChainManager.addProcessorChain("pipeline_1", qrsProcessorPtr1);
    qrsChainManager.addProcessorChain("pipeline_2", qrsProcessorPtr2);

    QrsProcessorPtr qrsProcessorPtrRet1
        = qrsChainManager.getProcessorChain("pipeline_1");
    ASSERT_TRUE(qrsProcessorPtrRet1.get());

    QrsProcessorPtr qrsProcessorPtrRet2
        = qrsChainManager.getProcessorChain("pipeline_2");
    ASSERT_TRUE(qrsProcessorPtrRet2.get());

    QrsProcessorPtr qrsProcessorPtrRet3
        = qrsChainManager.getProcessorChain("no_such_pipeline");
    ASSERT_TRUE(!qrsProcessorPtrRet3.get());
}

TEST_F(QrsChainManagerTest, testConstructChain) {
    HA3_LOG(DEBUG, "Begin Test!");

    QrsChainManager *qrsChainMgr = makeFakeChainManager();
    ASSERT_TRUE(qrsChainMgr);
    QrsChainManagerPtr qrsChainMgrPtr(qrsChainMgr);
    bool initRet = qrsChainMgrPtr->init();
    ASSERT_TRUE(initRet);

    QrsProcessorPtr qrsProcessorPtr
        = qrsChainMgrPtr->getProcessorChain("chain_1");
    ASSERT_TRUE(qrsProcessorPtr.get());

    CheckSummaryProcessor *checkSummaryProcessor =
        dynamic_cast<CheckSummaryProcessor*>(qrsProcessorPtr.get());
    ASSERT_TRUE(checkSummaryProcessor);
    ASSERT_EQ(string("CheckSummaryProcessor"),
                         checkSummaryProcessor->getName());

    QrsProcessorPtr qrsProcessorPtr_string
        = qrsProcessorPtr->getNextProcessor();
    FakeStringProcessor* stringProcessor =
        dynamic_cast<FakeStringProcessor *>(qrsProcessorPtr_string.get());
    ASSERT_TRUE(stringProcessor);
    ASSERT_EQ(string("FakeStringProcessor"),
                         stringProcessor->getName());

    //built-in
    QrsProcessorPtr qrsProcessorPtr_parser
        = qrsProcessorPtr_string->getNextProcessor();
    ASSERT_TRUE(qrsProcessorPtr_parser.get());
    ASSERT_EQ(string("RequestParserProcessor"),
                         qrsProcessorPtr_parser->getName());
    RequestParserProcessor* parserProcessor =
        dynamic_cast<RequestParserProcessor *>(qrsProcessorPtr_parser.get());
    ASSERT_TRUE(parserProcessor);


    QrsProcessorPtr qrsProcessorPtr_beforeValidate
        = qrsProcessorPtr_parser->getNextProcessor();
    ASSERT_TRUE(qrsProcessorPtr_beforeValidate.get());
    HA3_LOG(TRACE3, "processor_name:%s", qrsProcessorPtr_beforeValidate->getName().c_str());
    FakeLSDSProcessor* beforeValidateProcessor =
        dynamic_cast<FakeLSDSProcessor*>(qrsProcessorPtr_beforeValidate.get());
    ASSERT_TRUE(beforeValidateProcessor);
    ASSERT_EQ(string("FakeLSDSProcessor"),
                         beforeValidateProcessor->getName());


    //built-in
    QrsProcessorPtr qrsProcessorPtr_validator
        = qrsProcessorPtr_beforeValidate->getNextProcessor();
    ASSERT_TRUE(qrsProcessorPtr_validator.get());
    ASSERT_EQ(string("RequestValidateProcessor"),
                         qrsProcessorPtr_validator->getName());
    RequestValidateProcessor* validateProcessor =
        dynamic_cast<RequestValidateProcessor *>(qrsProcessorPtr_validator.get());
    ASSERT_TRUE(validateProcessor);


    QrsProcessorPtr qrsProcessorPtr_request
        = qrsProcessorPtr_validator->getNextProcessor();
    ASSERT_TRUE(qrsProcessorPtr_request.get());
    FakeRequestProcessor* requestProcessor =
        dynamic_cast<FakeRequestProcessor *>(qrsProcessorPtr_request.get());
    ASSERT_TRUE(requestProcessor);
    ASSERT_EQ(string("FakeRequestProcessor"),
                         requestProcessor->getName());

    //built-in
    QrsProcessorPtr qrsProcessorPtr_search
        = qrsProcessorPtr_request->getNextProcessor();
    ASSERT_TRUE(qrsProcessorPtr_search.get());
    QrsSearchProcessor* searchProcessor =
        dynamic_cast<QrsSearchProcessor *>(qrsProcessorPtr_search.get());
    ASSERT_TRUE(searchProcessor);
    ASSERT_EQ(string("QrsSearchProcessor"),
                         searchProcessor->getName());
}

TEST_F(QrsChainManagerTest, testDefaultChain) {
    HA3_LOG(DEBUG, "Begin Test!");

    QrsChainManager *qrsChainMgr = makeFakeChainManager();
    ASSERT_TRUE(qrsChainMgr);
    QrsChainManagerPtr qrsChainMgrPtr(qrsChainMgr);
    bool initRet = qrsChainMgrPtr->init();
    ASSERT_TRUE(initRet);

    QrsProcessorPtr qrsProcessorPtr
        = qrsChainMgrPtr->getProcessorChain("chain_2");
    CheckSummaryProcessor *checkSummaryProcessor =
        dynamic_cast<CheckSummaryProcessor*>(qrsProcessorPtr.get());
    ASSERT_TRUE(checkSummaryProcessor);
    ASSERT_EQ(string("CheckSummaryProcessor"),
                         checkSummaryProcessor->getName());

    //parser processor
    QrsProcessorPtr qrsProcessorPtr_parser
        = qrsProcessorPtr->getNextProcessor();
    ASSERT_TRUE(qrsProcessorPtr_parser.get());
    ASSERT_EQ(string("RequestParserProcessor"),
                         qrsProcessorPtr_parser->getName());
    RequestParserProcessor* parserProcessor =
        dynamic_cast<RequestParserProcessor *>(qrsProcessorPtr_parser.get());
    ASSERT_TRUE(parserProcessor);

    //request validate processor
    QrsProcessorPtr qrsProcessorPtr_validate
        = qrsProcessorPtr_parser->getNextProcessor();
    ASSERT_TRUE(qrsProcessorPtr_validate.get());
    RequestValidateProcessor* validateProcessor =
        dynamic_cast<RequestValidateProcessor*>(qrsProcessorPtr_validate.get());
    ASSERT_TRUE(validateProcessor);
    ASSERT_EQ(string("RequestValidateProcessor"),
                         validateProcessor->getName());

    //search processor
    QrsProcessorPtr qrsProcessorPtr_search
        = qrsProcessorPtr_validate->getNextProcessor();
    ASSERT_TRUE(qrsProcessorPtr_search.get());
    QrsSearchProcessor* searchProcessor =
        dynamic_cast<QrsSearchProcessor *>(qrsProcessorPtr_search.get());
    ASSERT_TRUE(searchProcessor);
    ASSERT_EQ(string("QrsSearchProcessor"),
                         searchProcessor->getName());
}

TEST_F(QrsChainManagerTest, testCreateProcessorsBeforeValidatePoint1) {
    HA3_LOG(DEBUG, "Begin Test!");

    map<string, vector<string> > chainProcessors;
    vector<string> &processors = chainProcessors["FakeQrsModule"];
    processors.push_back("FakeRequestProcessor");
    processors.push_back("FakeLSDSProcessor");
    QrsChainManagerPtr qrsChainMgrPtr(makeFakeChainManager(chainProcessors));

    ProcessorNameVec processNameVec(3);
    processNameVec[0] = (PageDistinctProcessor::PAGE_DIST_PROCESSOR_NAME);
    processNameVec[1] = ("FakeRequestProcessor");
    processNameVec[2] = ("FakeLSDSProcessor");

    QrsChainInfo chainInfo("chain_1");
    chainInfo.addPluginPoint(BEFORE_VALIDATE_POINT, processNameVec);

    QrsProcessorPtr chainHead;
    QrsProcessorPtr chainTail;

    bool ret = qrsChainMgrPtr->createProcessorsBeforeValidatePoint(
            chainInfo, chainHead, chainTail);
    ASSERT_TRUE(ret);
    QrsProcessorPtr currProcessorPtr = chainHead;

    checkProcessor<PageDistinctProcessor>(processNameVec[0], currProcessorPtr);

    currProcessorPtr = currProcessorPtr->getNextProcessor();
    checkProcessor<FakeRequestProcessor>(processNameVec[1], currProcessorPtr);

    currProcessorPtr = currProcessorPtr->getNextProcessor();
    checkProcessor<FakeLSDSProcessor>(processNameVec[2], currProcessorPtr);

    ASSERT_TRUE(!currProcessorPtr->getNextProcessor());
}

TEST_F(QrsChainManagerTest, testCreateProcessorsBeforeValidatePoint2) {
    HA3_LOG(DEBUG, "Begin Test!");

    map<string, vector<string> > chainProcessors;
    vector<string> &processors = chainProcessors["FakeQrsModule"];
    processors.push_back("FakeRequestProcessor");
    processors.push_back("FakeLSDSProcessor");
    QrsChainManagerPtr qrsChainMgrPtr(makeFakeChainManager(chainProcessors));

    ProcessorNameVec processNameVec(3);
    processNameVec[0] = ("FakeRequestProcessor");
    processNameVec[1] = (PageDistinctProcessor::PAGE_DIST_PROCESSOR_NAME);
    processNameVec[2] = ("FakeLSDSProcessor");

    QrsChainInfo chainInfo("chain_1");
    chainInfo.addPluginPoint(BEFORE_VALIDATE_POINT, processNameVec);

    QrsProcessorPtr chainHead;
    QrsProcessorPtr chainTail;

    bool ret = qrsChainMgrPtr->createProcessorsBeforeValidatePoint(
            chainInfo, chainHead, chainTail);
    ASSERT_TRUE(ret);
    QrsProcessorPtr currProcessorPtr = chainHead;

    checkProcessor<FakeRequestProcessor>(processNameVec[0], currProcessorPtr);

    currProcessorPtr = currProcessorPtr->getNextProcessor();
    checkProcessor<PageDistinctProcessor>(processNameVec[1], currProcessorPtr);

    currProcessorPtr = currProcessorPtr->getNextProcessor();
    checkProcessor<FakeLSDSProcessor>(processNameVec[2], currProcessorPtr);

    ASSERT_TRUE(!currProcessorPtr->getNextProcessor());
}

TEST_F(QrsChainManagerTest, testCreateProcessorsBeforeValidatePoint3) {
    HA3_LOG(DEBUG, "Begin Test!");

    map<string, vector<string> > chainProcessors;
    vector<string> &processors = chainProcessors["FakeQrsModule"];
    processors.push_back("FakeRequestProcessor");
    processors.push_back("FakeLSDSProcessor");
    QrsChainManagerPtr qrsChainMgrPtr(makeFakeChainManager(chainProcessors));

    ProcessorNameVec processNameVec(3);
    processNameVec[0] = ("FakeRequestProcessor");
    processNameVec[1] = ("FakeLSDSProcessor");
    processNameVec[2] = (PageDistinctProcessor::PAGE_DIST_PROCESSOR_NAME);

    QrsChainInfo chainInfo("chain_1");
    chainInfo.addPluginPoint(BEFORE_VALIDATE_POINT, processNameVec);

    QrsProcessorPtr chainHead;
    QrsProcessorPtr chainTail;

    bool ret = qrsChainMgrPtr->createProcessorsBeforeValidatePoint(
            chainInfo, chainHead, chainTail);
    ASSERT_TRUE(ret);
    QrsProcessorPtr currProcessorPtr = chainHead;

    checkProcessor<FakeRequestProcessor>(processNameVec[0], currProcessorPtr);

    currProcessorPtr = currProcessorPtr->getNextProcessor();
    checkProcessor<FakeLSDSProcessor>(processNameVec[1], currProcessorPtr);

    currProcessorPtr = currProcessorPtr->getNextProcessor();
    checkProcessor<PageDistinctProcessor>(processNameVec[2], currProcessorPtr);

    ASSERT_TRUE(!currProcessorPtr->getNextProcessor());
}

TEST_F(QrsChainManagerTest, testCreateProcessorsBeforeValidatePoint4) {
    HA3_LOG(DEBUG, "Begin Test!");

    map<string, vector<string> > chainProcessors;
    vector<string> &processors = chainProcessors["FakeQrsModule"];
    processors.push_back("FakeRequestProcessor");
    processors.push_back("FakeLSDSProcessor");
    QrsChainManagerPtr qrsChainMgrPtr(makeFakeChainManager(chainProcessors));

    ProcessorNameVec processNameVec(1);
    processNameVec[0] = (PageDistinctProcessor::PAGE_DIST_PROCESSOR_NAME);

    QrsChainInfo chainInfo("chain_1");
    chainInfo.addPluginPoint(BEFORE_VALIDATE_POINT, processNameVec);

    QrsProcessorPtr chainHead;
    QrsProcessorPtr chainTail;

    bool ret = qrsChainMgrPtr->createProcessorsBeforeValidatePoint(
            chainInfo, chainHead, chainTail);
    ASSERT_TRUE(ret);
    QrsProcessorPtr currProcessorPtr = chainHead;
    checkProcessor<PageDistinctProcessor>(processNameVec[0], currProcessorPtr);

    ASSERT_TRUE(!currProcessorPtr->getNextProcessor());
}

TEST_F(QrsChainManagerTest, testCreateProcessorsBeforeValidatePoint5) {
    HA3_LOG(DEBUG, "Begin Test!");

    map<string, vector<string> > chainProcessors;
    vector<string> &processors = chainProcessors["FakeQrsModule"];
    processors.push_back("FakeRequestProcessor");
    processors.push_back("FakeLSDSProcessor");
    QrsChainManagerPtr qrsChainMgrPtr(makeFakeChainManager(chainProcessors));

    ProcessorNameVec processNameVec(2);
    processNameVec[0] = ("FakeRequestProcessor");
    processNameVec[1] = ("FakeLSDSProcessor");

    QrsChainInfo chainInfo("chain_1");
    chainInfo.addPluginPoint(BEFORE_VALIDATE_POINT, processNameVec);

    QrsProcessorPtr chainHead;
    QrsProcessorPtr chainTail;

    bool ret = qrsChainMgrPtr->createProcessorsBeforeValidatePoint(
            chainInfo, chainHead, chainTail);
    ASSERT_TRUE(ret);
    QrsProcessorPtr currProcessorPtr = chainHead;

    checkProcessor<FakeRequestProcessor>(processNameVec[0], currProcessorPtr);

    currProcessorPtr = currProcessorPtr->getNextProcessor();
    checkProcessor<FakeLSDSProcessor>(processNameVec[1], currProcessorPtr);

    ASSERT_TRUE(!currProcessorPtr->getNextProcessor());
}

TEST_F(QrsChainManagerTest, testCreateProcessorsBeforeValidatePoint6) {
    HA3_LOG(DEBUG, "Begin Test!");

    map<string, vector<string> > chainProcessors;
    vector<string> &processors = chainProcessors["FakeQrsModule"];
    processors.push_back("FakeRequestProcessor");
    processors.push_back("FakeLSDSProcessor");
    QrsChainManagerPtr qrsChainMgrPtr(makeFakeChainManager(chainProcessors));

    QrsChainInfo chainInfo("chain_1");

    QrsProcessorPtr chainHead;
    QrsProcessorPtr chainTail;

    bool ret = qrsChainMgrPtr->createProcessorsBeforeValidatePoint(
            chainInfo, chainHead, chainTail);
    ASSERT_TRUE(ret);
    ASSERT_TRUE(!chainHead);
    ASSERT_TRUE(!chainTail);
}

TEST_F(QrsChainManagerTest, testInit) {
    HA3_LOG(DEBUG, "Begin Test!");

    QrsChainManager *qrsChainMgr = makeFakeChainManager();
    ASSERT_TRUE(qrsChainMgr);
    QrsChainManagerPtr qrsChainMgrPtr(qrsChainMgr);
    bool initRet = qrsChainMgrPtr->init();
    ASSERT_TRUE(initRet);

    QrsProcessorPtr processorPtr
        = qrsChainMgrPtr->getProcessorChain("chain_1");
    ASSERT_TRUE(processorPtr.get());
    // skip CheckSummaryProcessor
    processorPtr = processorPtr->getNextProcessor();
    ASSERT_TRUE(processorPtr.get());

    ASSERT_EQ(string("FakeStringProcessor"),processorPtr->getName());
    ASSERT_EQ(string("value1"), processorPtr->getParam("key1"));
    ASSERT_EQ(string("value2"), processorPtr->getParam("key2"));
}

TEST_F(QrsChainManagerTest, testInitFail) {
    HA3_LOG(DEBUG, "Begin Test!");

    QrsChainManager *qrsChainMgr = makeFakeChainManager();
    ASSERT_TRUE(qrsChainMgr);
    QrsChainInfo wrongChain("wrong_chain");
    wrongChain.addProcessor(BEFORE_PARSER_POINT,
                            "NotExistProcessor");
    qrsChainMgr->addQrsChainInfo(wrongChain);

    QrsChainManagerPtr qrsChainMgrPtr(qrsChainMgr);
    bool initRet = qrsChainMgrPtr->init();
    ASSERT_TRUE(!initRet);
}

QrsChainManager* QrsChainManagerTest::makeFakeChainManager(
        const map<string, vector<string> > &chainProcessors)
{
    QrsChainManager *qrsChainMgr = new QrsChainManager(
            _plugInManagerPtr, _resourceReaderPtr);
    qrsChainMgr->setClusterTableInfoMap(_clusterTableInfoMapPtr);
    map<string, vector<string> >::const_iterator it;
    for (it = chainProcessors.begin();
         it != chainProcessors.end(); ++it)
    {
        string moduleName = it->first;
        vector<string>::const_iterator processorNameIt;
        for (processorNameIt = it->second.begin();
             processorNameIt != it->second.end();
             ++processorNameIt)
        {
            ProcessorInfo processorInfo(*processorNameIt, moduleName);
            qrsChainMgr->addProcessorInfo(processorInfo);
        }
    }
    return qrsChainMgr;
}

QrsChainManager* QrsChainManagerTest::makeFakeChainManager() {
    map<string, vector<string> > chainProcessors;
    vector<string> &processors = chainProcessors["FakeQrsModule"];
    processors.push_back("FakeStringProcessor");
    processors.push_back("FakeRequestProcessor");
    processors.push_back("FakeLSDSProcessor");

    QrsChainManager *qrsChainMgr = makeFakeChainManager(chainProcessors);

    ProcessorInfo &processorInfo =
        qrsChainMgr->getProcessorInfo("FakeStringProcessor");
    KeyValueMap keyValues;
    keyValues["key1"] = "value1";
    keyValues["key2"] = "value2";
    processorInfo.setParams(keyValues);

    QrsChainInfo chainInfo1("chain_1");
    chainInfo1.addProcessor(BEFORE_PARSER_POINT,
                            "FakeStringProcessor");
    chainInfo1.addProcessor(BEFORE_VALIDATE_POINT,
                            "FakeLSDSProcessor");
    chainInfo1.addProcessor(BEFORE_SEARCH_POINT,
                            "FakeRequestProcessor");
    qrsChainMgr->addQrsChainInfo(chainInfo1);

    QrsChainInfo chainInfo2("chain_2");
    qrsChainMgr->addQrsChainInfo(chainInfo2);

    return qrsChainMgr;
}

QrsProcessorPtr QrsChainManagerTest::makeFakeChain()
{
    QrsProcessorPtr qrsProcessorPtr1(new FakeRawStringProcessor("rewrite_1"));
    QrsProcessorPtr qrsProcessorPtr2(new FakeRawStringProcessor("rewrite_2"));
    QrsProcessorPtr qrsProcessorPtr3(new FakeRawStringProcessor("rewrite_3"));
    qrsProcessorPtr1->setNextProcessor(qrsProcessorPtr2);
    qrsProcessorPtr2->setNextProcessor(qrsProcessorPtr3);
    return qrsProcessorPtr1;
}

common::RequestPtr QrsChainManagerTest::prepareEmptyRequest() {
    common::Request *request = new common::Request();
    common::RequestPtr requestPtr(request);
    return requestPtr;
}

ResultPtr QrsChainManagerTest::prepareEmptyResult() {
    Result *result = new Result();
    ResultPtr resultPtr(result);
    return resultPtr;
}

TEST_F(QrsChainManagerTest, testQrsRule) {
    QrsChainManager qrsManager(_plugInManagerPtr, _resourceReaderPtr);
    QRSRule qrsRule;
    qrsRule._retHitsLimit = 5000;
    qrsRule._connectionTimeout = -123123;
    qrsManager.setQRSRule(qrsRule);
    ASSERT_TRUE(qrsManager.getQRSRule()._retHitsLimit == 5000);
    ASSERT_TRUE(qrsManager.getQRSRule()._connectionTimeout == -123123);

}

template <typename ProcessorType>
void QrsChainManagerTest::checkProcessor(const string &processorName,
        QrsProcessorPtr processorPtr)
{
    ASSERT_TRUE(processorPtr.get());
    ASSERT_EQ(processorName, processorPtr->getName());
    ProcessorType *process =
        dynamic_cast<ProcessorType*>(processorPtr.get());
    ASSERT_TRUE(process != NULL);
}

END_HA3_NAMESPACE(qrs);
