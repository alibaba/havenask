#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsChainConfigurator.h>
#include <ha3/qrs/QrsChainConfig.h>
#include <ha3/test/test.h>

using namespace std;
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(qrs);

class QrsChainConfiguratorTest : public TESTBASE {
public:
    QrsChainConfiguratorTest();
    ~QrsChainConfiguratorTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, QrsChainConfiguratorTest);


QrsChainConfiguratorTest::QrsChainConfiguratorTest() { 
}

QrsChainConfiguratorTest::~QrsChainConfiguratorTest() { 
}

void QrsChainConfiguratorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void QrsChainConfiguratorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}
/*
TEST_F(QrsChainConfiguratorTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string basePath = TEST_DATA_PATH;
    string confFile = basePath + "/conf/qrs_chain.conf";
    HA3_LOG(TRACE3, "configfile: %s", confFile.c_str());

    QrsChainConfig conf;
    QrsChainConfigurator qrsChainConfigurator;
    bool parseRet = qrsChainConfigurator.parseFromFile(confFile, conf);
    ASSERT_TRUE(parseRet);

    /////////////// test ProcessorInfo /////////////
    map<string, ProcessorInfo> processorInfoMap = conf.processorInfoMap;

    string pName = "FakeStringProcessor";
    string moduleName = "FakeQrsModule";
    ASSERT_TRUE(processorInfoMap.find(pName) != processorInfoMap.end());
    ProcessorInfo &processorInfo = processorInfoMap[pName];
    ASSERT_EQ(pName, processorInfo.getProcessorName());
    ASSERT_EQ(moduleName, processorInfo.getModuleName());        
    ASSERT_EQ(string("10"), processorInfo.getParam("k1"));
    ASSERT_EQ(string("4"), processorInfo.getParam("k2"));

    pName = "FakeRequestProcessor";
    ASSERT_TRUE(processorInfoMap.find(pName) != processorInfoMap.end());
    processorInfo = processorInfoMap[pName];
    ASSERT_EQ(pName, processorInfo.getProcessorName());
    ASSERT_EQ(moduleName, processorInfo.getModuleName());        
    ASSERT_EQ(string("111"), processorInfo.getParam("k1"));
    ASSERT_EQ(string("222"), processorInfo.getParam("k2"));

    pName = "FakeResultProcessor";
    ASSERT_TRUE(processorInfoMap.find(pName) != processorInfoMap.end());
    processorInfo = processorInfoMap[pName];
    ASSERT_EQ(pName, processorInfo.getProcessorName());
    ASSERT_EQ(moduleName, processorInfo.getModuleName());        
    ASSERT_EQ(string("teststring"), processorInfo.getParam("k3"));

    pName = "FakeLSDSProcessor";
    ASSERT_TRUE(processorInfoMap.find(pName) != processorInfoMap.end());
    processorInfo = processorInfoMap[pName];
    ASSERT_EQ(pName, processorInfo.getProcessorName());
    ASSERT_EQ(moduleName, processorInfo.getModuleName());        
    ASSERT_EQ(string("abc"), processorInfo.getParam("lsds"));

            
    /////////////// test QrsChainInfo /////////////
    map<string, QrsChainInfo> chainInfoMap = conf.chainInfoMap;
    ASSERT_TRUE(chainInfoMap.find("web") != chainInfoMap.end());

    ProcessorNameVec qrsChainPoint
        = chainInfoMap["web"].getPluginPoint(BEFORE_PARSER_POINT);
    ASSERT_EQ((size_t)1, qrsChainPoint.size());
    ASSERT_EQ(string("FakeStringProcessor"), qrsChainPoint[0]);

    qrsChainPoint = chainInfoMap["web"].getPluginPoint(BEFORE_VALIDATE_POINT);
    ASSERT_EQ((size_t)1, qrsChainPoint.size());
    ASSERT_EQ(string("FakeLSDSProcessor"), qrsChainPoint[0]);

    qrsChainPoint = chainInfoMap["web"].getPluginPoint(BEFORE_SEARCH_POINT);
    ASSERT_EQ((size_t)2, qrsChainPoint.size());
    ASSERT_EQ(string("FakeRequestProcessor"), qrsChainPoint[0]);
    ASSERT_EQ(string("FakeRequestProcessor2"), qrsChainPoint[1]);
}
*/

END_HA3_NAMESPACE(qrs);

