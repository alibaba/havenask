#include "swift/config/AdminConfig.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "swift/config/AuthorizerInfo.h"
#include "unittest/unittest.h"

namespace swift {
namespace config {

class AdminConfigTest : public TESTBASE {};

using namespace std;

TEST_F(AdminConfigTest, testFailed) {
    AdminConfig *config = AdminConfig::loadConfig("./a");
    ASSERT_FALSE(config);
}

TEST_F(AdminConfigTest, testParseConfig) {
    string confPath = GET_TEST_DATA_PATH() + "config/admin_config_test";

    AdminConfig *config = AdminConfig::loadConfig(confPath);
    ASSERT_TRUE(config != NULL);
    ASSERT_EQ(string("userName"), config->userName);
    ASSERT_EQ(string("serviceName"), config->serviceName);
    ASSERT_EQ(string("path/to/hippo/leader/elector/root"), config->hippoRoot);
    ASSERT_EQ(string("path/to/zk/root"), config->zkRoot);
    ASSERT_EQ((uint32_t)14, config->brokerCount);
    ASSERT_EQ((uint32_t)5, config->adminCount);
    ASSERT_FALSE(config->backup);
    map<string, uint32_t> groupBroker = config->getGroupBrokerCountMap();
    ASSERT_EQ((size_t)3, groupBroker.size());
    ASSERT_EQ((uint32_t)10, groupBroker["default"]);
    ASSERT_EQ((uint32_t)2, groupBroker["abc"]);
    ASSERT_EQ((uint32_t)2, groupBroker["ab"]);
    map<string, uint32_t> netPriority = config->getGroupNetPriorityMap();
    ASSERT_EQ((size_t)3, netPriority.size());
    ASSERT_EQ((uint32_t)123, netPriority["default"]);
    ASSERT_EQ((uint32_t)1, netPriority["a"]);
    ASSERT_EQ((uint32_t)2, netPriority["b"]);

    ASSERT_EQ((uint16_t)10010, config->amonitorPort);
    ASSERT_EQ((int32_t)5, config->leaderLeaseTime);
    ASSERT_EQ((int32_t)3, config->leaderLoopInterval);
    ASSERT_EQ((int64_t)25000, config->syncTopicInfoInterval);
    ASSERT_EQ((uint32_t)10, config->dispatchIntervalMs);
    ASSERT_EQ((uint32_t)11, config->candidateWaitTimeMs);
    ASSERT_EQ((int64_t)1200, config->adjustResourceDuration);
    ASSERT_EQ((int32_t)11, config->threadNum);
    ASSERT_EQ((int32_t)110, config->queueSize);
    ASSERT_EQ((int32_t)1, config->ioThreadNum);
    ASSERT_EQ((int32_t)10, config->anetTimeoutLoopIntervalMs);
    ASSERT_TRUE(config->promoteIoThreadPriority);
    ASSERT_TRUE(config->exclusiveListenThread);
    ASSERT_EQ((int32_t)111, config->adminThreadNum);
    ASSERT_EQ((int32_t)1100, config->adminQueueSize);
    ASSERT_EQ((int32_t)11, config->adminHttpThreadNum);
    ASSERT_EQ((int32_t)110, config->adminHttpQueueSize);
    ASSERT_EQ((int32_t)10000, config->heartbeatIntervalInUs);
    ASSERT_EQ(true, config->authConf._enable);
    std::map<std::string, std::string> kvMap1 = {{"user1", "passwd1"}, {"user2", "passwd2"}};
    std::map<std::string, std::string> kvMap2 = {{"user3", "passwd3"}, {"user4", "passwd4"}};
    ASSERT_EQ(kvMap1, config->authConf._sysUsers);
    ASSERT_EQ(kvMap2, config->authConf._normalUsers);
    ASSERT_EQ(98765, config->maxTopicAclSyncIntervalUs);

    ASSERT_EQ((int32_t)3, config->adminIoThreadNum);

    ASSERT_EQ((double)33, config->reserveDataHour);
    ASSERT_EQ((double)25, config->cleanDataIntervalHour);
    ASSERT_EQ((double)100, config->delExpiredTopicIntervalSec);
    ASSERT_EQ(string("pangu://localcluster:10240/home"), config->dfsRoot);
    ASSERT_EQ(string("pangu://localcluster:10240/home/extend"), config->extendDfsRoot);
    ASSERT_EQ(string("pangu://localcluster:10240/home/todel"), config->todelDfsRoot);
    ASSERT_EQ((size_t)3, config->syncAdminInfoPathVec.size());
    ASSERT_DOUBLE_EQ(0.8, config->decsionThreshold);
    ASSERT_EQ(confPath, config->configPath);
    ASSERT_EQ(string("admin_config_test"), config->configVersion);
    ASSERT_EQ(100000, config->syncAdminInfoInterval);
    ASSERT_EQ(int64_t(250 * 1000 * 1000), config->syncHeartbeatInterval);
    ASSERT_EQ(BROKER_EL_ALL, config->getExclusiveLevel());
    ASSERT_EQ(uint32_t(0), config->exclusiveLevel);
    ASSERT_EQ(int64_t(60), config->forceScheduleTimeSecond);
    ASSERT_EQ(10, config->recordLocalFile);
    ASSERT_EQ(int64_t(900), config->releaseDeadWorkerTimeSecond);
    ASSERT_EQ(int64_t(10), config->topicResourceLimit);
    ASSERT_EQ(int64_t(2), config->workerPartitionLimit);
    ASSERT_EQ(int64_t(533), config->minMaxPartitionBuffer);
    vector<pair<string, string>> topicGroupInfo;
    topicGroupInfo.emplace_back("aa", "g1");
    topicGroupInfo.emplace_back("bb", "g2");
    ASSERT_EQ(topicGroupInfo, config->topicGroupVec);
    vector<pair<string, vector<string>>> topicOwnerInfo;
    vector<string> owners1 = {"linson"};
    vector<string> owners2 = {"dongyu", "shenfu"};
    topicOwnerInfo.emplace_back("aa", owners1);
    topicOwnerInfo.emplace_back("bb", owners2);
    ASSERT_EQ(topicOwnerInfo, config->topicOwnerVec);
    ASSERT_EQ(200, config->reserveBackMetaCount);
    ASSERT_EQ("", config->backMetaPath);
    ASSERT_EQ(60, config->requestProcessTimeRatio);
    ASSERT_EQ(222, config->brokerNetLimitMB);
    ASSERT_TRUE(config->updatePartResource);
    ASSERT_TRUE(config->localMode);
    ASSERT_TRUE(config->multiThreadDispatchTask);
    ASSERT_EQ(20, config->dispatchTaskThreadNum);
    ASSERT_EQ(100, config->noUseTopicExpireTimeSec);
    ASSERT_EQ(200, config->noUseTopicDeleteIntervalSec);
    ASSERT_EQ(300, config->missTopicRecoverIntervalSec);
    ASSERT_EQ(20, config->maxAllowNouseFileNum);
    ASSERT_EQ(123 * 3600, config->brokerCfgTTlSec);
    ASSERT_EQ(10, config->maxRestartCountInLocal);
    ASSERT_TRUE(config->fastRecover);
    ASSERT_FALSE(config->useRecommendPort);
    vector<string> CADTopicPatterns = {"_test1_", "test2"};
    ASSERT_EQ(CADTopicPatterns, config->getCleanAtDeleteTopicPatterns());

    delete config;
}

TEST_F(AdminConfigTest, testParseConfigOpt) {
    string confPath = GET_TEST_DATA_PATH() + "config/admin_config_test_opt";
    AdminConfig *config = AdminConfig::loadConfig(confPath);
    ASSERT_TRUE(config != NULL);
    ASSERT_EQ(300, config->reserveBackMetaCount);
    ASSERT_EQ(1000, config->brokerNetLimitMB);
    ASSERT_TRUE(!config->updatePartResource);
    ASSERT_FALSE(config->localMode);
    ASSERT_EQ(0, config->noUseTopicExpireTimeSec);
    ASSERT_EQ(0, config->noUseTopicDeleteIntervalSec);
    ASSERT_EQ(0, config->missTopicRecoverIntervalSec);
    ASSERT_EQ(10, config->maxAllowNouseFileNum);
    ASSERT_EQ("pangu://localcluster:10240/back", config->backMetaPath);
    ASSERT_EQ(std::numeric_limits<int64_t>::max(), config->brokerCfgTTlSec);
    ASSERT_FALSE(config->promoteIoThreadPriority);
    ASSERT_FALSE(config->exclusiveListenThread);
    ASSERT_FALSE(config->fastRecover);
    delete config;
}

TEST_F(AdminConfigTest, testParseConfigError) {
    string confPath = GET_TEST_DATA_PATH() + "config/admin_config_test_error";
    AdminConfig *config = AdminConfig::loadConfig(confPath);
    ASSERT_TRUE(config == NULL);
}

TEST_F(AdminConfigTest, testParseConfigProcessRatio) {
    string confPath = GET_TEST_DATA_PATH() + "config/admin_config_process_ratio";
    AdminConfig *config = AdminConfig::loadConfig(confPath);
    ASSERT_TRUE(config == NULL);
}

TEST_F(AdminConfigTest, testValidate) {
    string confPath = GET_TEST_DATA_PATH() + "config/admin_config_test";

    AdminConfig *config = AdminConfig::loadConfig(confPath);
    ASSERT_TRUE(config != NULL);
    ASSERT_TRUE(config->validate());
    config->hippoRoot = "";
    ASSERT_TRUE(config->validate());
    config->localMode = false;
    ASSERT_FALSE(config->validate());
    config->localMode = true;
    config->veticalGroupBrokerCountMap["default"] = 11;
    ASSERT_FALSE(config->validate());
    config->veticalGroupBrokerCountMap["default"] = 10;
    ASSERT_TRUE(config->validate());
    config->cleanAtDeleteTopicPatterns.push_back("");
    ASSERT_FALSE(config->validate());
    config->cleanAtDeleteTopicPatterns.clear();
    ASSERT_TRUE(config->validate());
    delete config;
}

TEST_F(AdminConfigTest, testGetConfigStr) {
    AdminConfig configDefault;
    string expectStr;

    expectStr = "userName: serviceName: hippoRoot: zkRoot: configVersion: amonitorPort:10086 "
                "brokerCount:0 adminCount:0 dispatchIntervalMs:1000 candidateWaitTimeMs:1000 brokerNetLimitMB:1000 "
                "dfsRoot: extendDfsRoot: todelDfsRoot: configPath: backMetaPath: "
                "cleanDataIntervalHour:1 delExpiredTopicIntervalSec:300 "
                "reserveDataHour:8760 queueSize:200 threadNum:16 ioThreadNum:1 "
                "anetTimeoutLoopIntervalMs:0 adminQueueSize:200 adminThreadNum:16 "
                "adminHttpQueueSize:200 adminHttpThreadNum:16 adminIoThreadNum:1 "
                "decsionThreshold:0.5 leaderLeaseTime:60 leaderLoopInterval:2 "
                "syncAdminInfoPathVec:[] syncAdminInfoInterval:2000000 "
                "syncTopicInfoInterval:5000000 syncHeartbeatInterval:300000000 "
                "groupBrokerCountMap:[] veticalGroupBrokerCountMap:[] groupNetPriorityMap:[] "
                "adjustResourceDuration:20000 exclusiveLevel:0 "
                "forceScheduleTimeSecond:-1 releaseDeadWorkerTimeSecond:600 "
                "workerPartitionLimit:-1 topicResourceLimit:-1 minMaxPartitionBuffer:256 "
                "brokerCfgTTlSec:9223372036854775807 recordLocalFile:-1 "
                "reserveBackMetaCount:200 requestProcessTimeRatio:60 "
                "noUseTopicExpireTimeSec:0 noUseTopicDeleteIntervalSec:0 "
                "missTopicRecoverIntervalSec:0 maxAllowNouseFileNum:10 "
                "autoUpdatePartResource:false localMode:false workDir: logConfFile: multiThreadDispatchTask:false "
                "dispatchTaskThreadNum:10 heartbeatIntervalInUs:0 topicScdType: dealErrorBroker:false "
                "errorBrokerDealRatio:0.1 reportZombieUrl: zfsTimeout:5000000 commitDelayThreshold:1800 "
                "brokerUnknownTimeout:5000000 deadBrokerTimeoutSec:180 forbidCampaignTimeMs:10000 "
                "useRecommendPort:true chgPartcntIntervalUs:2000000 maxRestartCountInLocal:-1 "
                "backup:false initialMasterVersion:0 forceSyncLeaderInfoInterval:30000000 mirrorZkRoot: "
                "longPeriodIntervalSec:21600 "
                "heartbeatThreadNum:1 heartbeatQueueSize:1000 maxTopicAclSyncIntervalUs:10000000 "
                "enableAuthentication:false "
                "authentications:[] topicGroupVec:[] topicOwnerVec:[] cleanAtDeleteTopicPatterns:[] "
                "metaInfoReplicateInterval:30000000";

    EXPECT_EQ(expectStr, configDefault.getConfigStr());

    string confPath = GET_TEST_DATA_PATH() + "config/admin_config_test";
    cout << confPath << endl;
    AdminConfig *config = AdminConfig::loadConfig(confPath);
    config->configPath = "local://xxx";

    expectStr = "userName:userName serviceName:serviceName "
                "hippoRoot:path/to/hippo/leader/elector/root "
                "zkRoot:path/to/zk/root configVersion:admin_config_test "
                "amonitorPort:10010 brokerCount:14 adminCount:5 "
                "dispatchIntervalMs:10 candidateWaitTimeMs:11 brokerNetLimitMB:222 "
                "dfsRoot:pangu://localcluster:10240/home "
                "extendDfsRoot:pangu://localcluster:10240/home/extend "
                "todelDfsRoot:pangu://localcluster:10240/home/todel "
                "configPath:local://xxx backMetaPath: cleanDataIntervalHour:25 "
                "delExpiredTopicIntervalSec:100 reserveDataHour:33 "
                "queueSize:110 threadNum:11 ioThreadNum:1 "
                "anetTimeoutLoopIntervalMs:10 adminQueueSize:1100 "
                "adminThreadNum:111 adminHttpQueueSize:110 adminHttpThreadNum:11 "
                "adminIoThreadNum:3 decsionThreshold:0.8 leaderLeaseTime:5 "
                "leaderLoopInterval:3 syncAdminInfoPathVec:[a b c ] "
                "syncAdminInfoInterval:100000 syncTopicInfoInterval:25000 "
                "syncHeartbeatInterval:250000000 "
                "groupBrokerCountMap:[ab:2 abc:2 default:10 ] "
                "veticalGroupBrokerCountMap:[ab:2 abc:2 default:10 other:5 ] "
                "groupNetPriorityMap:[a:1 b:2 default:123 ] "
                "adjustResourceDuration:1200 exclusiveLevel:0 "
                "forceScheduleTimeSecond:60 releaseDeadWorkerTimeSecond:900 "
                "workerPartitionLimit:2 topicResourceLimit:10 minMaxPartitionBuffer:533 "
                "brokerCfgTTlSec:442800 recordLocalFile:10 reserveBackMetaCount:200 "
                "requestProcessTimeRatio:60 noUseTopicExpireTimeSec:100 "
                "noUseTopicDeleteIntervalSec:200 missTopicRecoverIntervalSec:300 "
                "maxAllowNouseFileNum:20 autoUpdatePartResource:true "
                "localMode:true workDir: logConfFile: multiThreadDispatchTask:true dispatchTaskThreadNum:20 "
                "heartbeatIntervalInUs:10000 topicScdType:vetical dealErrorBroker:true "
                "errorBrokerDealRatio:0.2 reportZombieUrl:http://test.net/api/report zfsTimeout:3000 "
                "commitDelayThreshold:4000 brokerUnknownTimeout:1000 deadBrokerTimeoutSec:20 forbidCampaignTimeMs:5000 "
                "useRecommendPort:false chgPartcntIntervalUs:300 maxRestartCountInLocal:10 "
                "backup:false initialMasterVersion:0 forceSyncLeaderInfoInterval:12345678 "
                "mirrorZkRoot:zfs://xxx-aa-bb "
                "longPeriodIntervalSec:4321 heartbeatThreadNum:10 heartbeatQueueSize:10000 "
                "maxTopicAclSyncIntervalUs:98765 enableAuthentication:true "
                "authentications:[user1:passwd1 user2:passwd2 ] topicGroupVec:[aa:g1 bb:g2 ] "
                "topicOwnerVec:[aa:(linson ) bb:(dongyu shenfu ) ] cleanAtDeleteTopicPatterns:[_test1_ test2 ] "
                "metaInfoReplicateInterval:100";

    EXPECT_EQ(expectStr, config->getConfigStr());
    delete config;
}

} // namespace config
} // namespace swift
