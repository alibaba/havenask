#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/QrsConfig.h>
#include <suez/turing/common/FileUtil.h>
#include <ha3/test/test.h>
#include <string>
#include <autil/legacy/jsonizable.h>

using namespace std;
using namespace build_service::plugin;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace suez::turing;
BEGIN_HA3_NAMESPACE(config);

class QrsConfigTest : public TESTBASE {
public:
    QrsConfigTest();
    ~QrsConfigTest();
public:
    void setUp();
    void tearDown();
protected:
    void assertQrsChainInfo(const QrsConfig &info);
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(config, QrsConfigTest);


QrsConfigTest::QrsConfigTest() { 
}

QrsConfigTest::~QrsConfigTest() { 
}

void QrsConfigTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void QrsConfigTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(QrsConfigTest, testJsonize) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string str = FileUtil::readFile(TEST_DATA_PATH"/qrs_config_test/test_jsonize_qrs.json");

    QrsConfig info;
    FromJsonString(info, str);
    assertQrsChainInfo(info);

    ASSERT_EQ(std::string("z_speed_compress"), info._resultCompress._compressType);
    ASSERT_EQ(std::string("z_speed_compress"), info._requestCompress._compressType);

    QrsConfig info1 = info;
    ASSERT_EQ(std::string("z_speed_compress"), info1._resultCompress._compressType);
    ASSERT_EQ(std::string("z_speed_compress"), info1._requestCompress._compressType);

    ASSERT_EQ(uint32_t(2), info._qrsRule._replicaCount);
    ASSERT_EQ(string("qrs"), info._qrsRule._resourceType.toString());

    string jsonString = ToJsonString(info);

    HA3_LOG(DEBUG,"before FromJsonString('%s')", jsonString.c_str());
    FromJsonString(info, jsonString);
    assertQrsChainInfo(info);
    ASSERT_EQ(POOL_TRUNK_SIZE, info._poolConfig._poolTrunkSize);
    ASSERT_EQ(POOL_RECYCLE_SIZE_LIMIT, info._poolConfig._poolRecycleSizeLimit);
    ASSERT_EQ(POOL_MAX_COUNT, info._poolConfig._poolMaxCount);
}

TEST_F(QrsConfigTest, testJsonizeWithPoolConfig) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string str = FileUtil::readFile(TEST_DATA_PATH"/qrs_config_test/test_jsonize_qrs_with_poolconfig.json");

    QrsConfig info;
    FromJsonString(info, str);
    assertQrsChainInfo(info);

    ASSERT_EQ(std::string("z_speed_compress"), info._resultCompress._compressType);
    ASSERT_EQ(std::string("z_speed_compress"), info._requestCompress._compressType);

    QrsConfig info1 = info;
    ASSERT_EQ(std::string("z_speed_compress"), info1._resultCompress._compressType);
    ASSERT_EQ(std::string("z_speed_compress"), info1._requestCompress._compressType);

    ASSERT_EQ(uint32_t(2), info._qrsRule._replicaCount);
    ASSERT_EQ(string("qrs"), info._qrsRule._resourceType.toString());

    string jsonString = ToJsonString(info);

    HA3_LOG(DEBUG,"before FromJsonString('%s')", jsonString.c_str());
    FromJsonString(info, jsonString);
    assertQrsChainInfo(info);
    ASSERT_EQ(400, info._poolConfig._poolTrunkSize);
    ASSERT_EQ(800, info._poolConfig._poolRecycleSizeLimit);
    ASSERT_EQ(300, info._poolConfig._poolMaxCount);
}

TEST_F(QrsConfigTest, testJsonizeEmpty) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string str = "{}";

    QrsConfig info;
    FromJsonString(info, str);
    ASSERT_EQ((uint32_t)QRS_RETURN_HITS_LIMIT, info._qrsRule._retHitsLimit);
    ASSERT_EQ((int32_t)QRS_ARPC_CONNECTION_TIMEOUT, info._qrsRule._connectionTimeout);
    ASSERT_EQ((uint32_t)QRS_ARPC_REQUEST_QUEUE_SIZE, info._qrsRule._requestQueueSize);
    ASSERT_EQ(DEFAULT_TASK_QUEUE_NAME, 
                         info._qrsRule._searchTaskQueueRule.phaseOneTaskQueue);
    ASSERT_EQ(DEFAULT_TASK_QUEUE_NAME, 
                         info._qrsRule._searchTaskQueueRule.phaseTwoTaskQueue);
    ASSERT_EQ(uint32_t(QRS_REPLICA_COUNT),
                         info._qrsRule._replicaCount);
    ASSERT_EQ(string(DEFAULT_RESOURCE_TYPE),
                         info._qrsRule._resourceType.toString());


    ASSERT_EQ((size_t)0, info._modules.size());
    ASSERT_EQ((size_t)0, info._processorInfos.size());

    vector<QrsChainInfo> qrsChainInfos = info._qrsChainInfos;
    ASSERT_EQ((size_t)1, qrsChainInfos.size());
    ASSERT_EQ(string("DEFAULT"), qrsChainInfos[0]._chainName);
    ASSERT_EQ((size_t)0, qrsChainInfos[0]._pluginPoints.size());
    ASSERT_EQ(std::string("no_compress"), info._resultCompress._compressType);
    ASSERT_EQ(std::string("no_compress"), info._requestCompress._compressType);

    ASSERT_EQ((size_t)0, info._metricsSrcWhiteList.size());
}

TEST_F(QrsConfigTest, testJsonizeWithoutSomeSection) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string str = FileUtil::readFile(TEST_DATA_PATH"/qrs_config_test/test_jsonize_without_some_section_qrs.json");
    QrsConfig info;
    FromJsonString(info, str);

    ASSERT_EQ((uint32_t)QRS_RETURN_HITS_LIMIT, info._qrsRule._retHitsLimit);
    ASSERT_EQ(QRS_ARPC_CONNECTION_TIMEOUT, info._qrsRule._connectionTimeout);

    ASSERT_EQ((size_t)0, info._modules.size());

    ASSERT_EQ((size_t)1, info._processorInfos.size());
    ASSERT_EQ(string("FakeStringProcessor"), info._processorInfos[0]._processorName);

    vector<QrsChainInfo> qrsChainInfos = info._qrsChainInfos;
    ASSERT_EQ((size_t)1, qrsChainInfos.size());
    ASSERT_EQ(string("web"), qrsChainInfos[0]._chainName);
    QrsPluginPoints pluginPoints = qrsChainInfos[0]._pluginPoints;
    ASSERT_EQ((size_t)1, pluginPoints.size());
    ProcessorNameVec nameVec = pluginPoints["RAW_STRING_PROCESS"];
    ASSERT_EQ((size_t)1, nameVec.size());
    ASSERT_EQ(string("FakeStringProcessor"), nameVec[0]);

    ASSERT_EQ((size_t)0, info._metricsSrcWhiteList.size());
}


void QrsConfigTest::assertQrsChainInfo(const QrsConfig &info) { 
    QRSRule qrsRule = info._qrsRule;
    ASSERT_EQ((uint32_t)2000, qrsRule._retHitsLimit);
    ASSERT_EQ((int32_t)80, qrsRule._connectionTimeout);
    ASSERT_EQ(string("search_queue"), 
                         qrsRule._searchTaskQueueRule.phaseOneTaskQueue);
    ASSERT_EQ(string("summary_queue"), 
                         qrsRule._searchTaskQueueRule.phaseTwoTaskQueue);

    vector<ModuleInfo> modules = info._modules;
    ASSERT_EQ((size_t)1, modules.size());
    ModuleInfo moduleInfo = modules[0];
    ASSERT_EQ(string("qrs_processors"), moduleInfo.moduleName);
    ASSERT_EQ(string("qrs_processor_1.so"), moduleInfo.modulePath);
    KeyValueMap parameters = moduleInfo.parameters;
    ASSERT_EQ((size_t)2, parameters.size());
    ASSERT_EQ(string("value"), parameters["key"]);
    ASSERT_EQ(string("value2"), parameters["key2"]);

    vector<ProcessorInfo> processorInfos = info._processorInfos;
    ASSERT_EQ((size_t)3, processorInfos.size());
    ASSERT_EQ(string("FakeStringProcessor"), processorInfos[0]._processorName);
    ASSERT_EQ(string("qrs_processor"), processorInfos[0]._moduleName);
    ASSERT_EQ((size_t)2, processorInfos[0]._params.size());
    ASSERT_EQ(string("10"), processorInfos[0]._params["k1"]);
    ASSERT_EQ(string("4"), processorInfos[0]._params["k2"]);

    ASSERT_EQ(string("FakeStringProcessor1"), processorInfos[1]._processorName);
    ASSERT_EQ(string("qrs_processor"), processorInfos[1]._moduleName);
    ASSERT_EQ((size_t)1, processorInfos[1]._params.size());
    ASSERT_EQ(string("teststring"), processorInfos[1]._params["k3"]);

    ASSERT_EQ(string("FakeStringProcessor2"), processorInfos[2]._processorName);
    ASSERT_EQ(string("qrs_processor"), processorInfos[2]._moduleName);
    ASSERT_EQ((size_t)1, processorInfos[2]._params.size());
    ASSERT_EQ(string("teststring"), processorInfos[2]._params["k3"]);

    vector<QrsChainInfo> qrsChainInfos = info._qrsChainInfos;
    ASSERT_EQ((size_t)1, qrsChainInfos.size());
    ASSERT_EQ(string("web"), qrsChainInfos[0]._chainName);
    QrsPluginPoints pluginPoints = qrsChainInfos[0]._pluginPoints;
    ASSERT_EQ((size_t)2, pluginPoints.size());

    ProcessorNameVec nameVec = pluginPoints["RAW_STRING_PROCESS"];
    ASSERT_EQ((size_t)1, nameVec.size());
    ASSERT_EQ(string("FakeStringProcessor"), nameVec[0]);
    nameVec = pluginPoints["REQUEST_PROCESS"];
    ASSERT_EQ((size_t)2, nameVec.size());
    ASSERT_EQ(string("FakeStringProcessor1"), nameVec[0]);
    ASSERT_EQ(string("FakeStringProcessor2"), nameVec[1]);

    vector<string> whiteList = info._metricsSrcWhiteList;
    ASSERT_EQ((size_t)2, whiteList.size());
    ASSERT_EQ(string("src1"), whiteList[0]);
    ASSERT_EQ(string("src2"), whiteList[1]);
}



END_HA3_NAMESPACE(config);

