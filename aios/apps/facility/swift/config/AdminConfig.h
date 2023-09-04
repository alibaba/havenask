/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <map>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "swift/config/AuthorizerInfo.h"

namespace swift {
namespace config {
enum BrokerExclusiveLevel {
    BROKER_EL_ALL = 0,
    BROKER_EL_GROUP = 1,
    BROKER_EL_NONE = 2,
    BROKER_EL_UNKNOWN = 3,
};

class AdminConfig {
public:
    AdminConfig();
    ~AdminConfig();

private:
    AdminConfig(const AdminConfig &);
    AdminConfig &operator=(const AdminConfig &);

public:
    static AdminConfig *loadConfig(const std::string &configPath);
    static std::string parseConfigVersion(const std::string &configPath);
    std::string getApplicationId() const;
    std::string getUserName() const;
    std::string getServiceName() const;
    std::string getHippoRoot() const;
    std::string getZkRoot() const;
    uint16_t getAmonitorPort() const;
    uint32_t getBrokerCount() const;
    uint32_t getAdminCount() const;
    uint32_t getDispatchIntervalMs() const;
    uint32_t getCandidateWaitTimeMs() const;
    uint32_t getBrokerNetLimitMB() const;
    double getCleanDataIntervalHour() const;
    double getDelExpiredTopicIntervalSec() const;
    double getReserveDataByHour() const;
    std::string getDfsRoot() const;
    std::string getExtendDfsRoot() const;
    std::string getTodelDfsRoot() const;
    std::string getConfigPath() const;
    std::string getBackMetaPath() const;
    std::string getConfigVersion() const;
    double getDecsionThreshold() const;
    int getQueueSize() const;
    int getThreadNum() const;
    int getAnetTimeoutLoopIntervalMs() const;
    bool getExclusiveListenThread() const;
    bool getPromoteIoThreadPriority() const;
    int getIoThreadNum() const;
    int getAdminQueueSize() const;
    int getAdminThreadNum() const;
    int getAdminHttpQueueSize() const;
    int getAdminHttpThreadNum() const;
    int getAdminIoThreadNum() const;
    int32_t getLeaderLeaseTime() const;
    double getLeaderLoopInterval() const;
    const std::vector<std::string> &getSyncAdminInfoPath();
    int64_t getSyncAdminInfoInterval();
    int64_t getSyncTopicInfoInterval();
    int64_t getSyncHeartbeatInterval();
    int64_t getAdjustResourceDuration();
    const std::map<std::string, uint32_t> &getGroupBrokerCountMap();
    BrokerExclusiveLevel getExclusiveLevel() const;
    int64_t getForceScheduleTimeSecond() const;
    int getRecordLocalFile() const;
    int64_t getReleaseDeadWorkerTimeSecond() const;
    int64_t getWorkPartitionLimit() const;
    int64_t getTopicResourceLimit() const;
    int64_t getMinMaxPartitionBufferSizeMb() const;
    int64_t getBrokerCfgTTlSec() const;
    uint32_t getReserveBackMetaCount() const;
    uint32_t getRequestProcessTimeRatio() const;
    uint32_t getNoUseTopicExpireTimeSec() const;
    uint32_t getNoUseTopicDeleteIntervalSec() const;
    uint32_t getMissTopicRecoverIntervalSec() const;
    uint32_t getMaxAllowNouseFileNum() const;
    bool autoUpdatePartResource() const;
    bool isLocalMode() const;
    bool enableMultiThreadDispatchTask() const;
    uint32_t getDispatchTaskThreadNum() const;
    bool enableFastRecover() const;
    uint32_t getHeartbeatIntervalInUs() const;
    const std::vector<std::pair<std::string, std::string>> &getTopicGroupVec();
    const std::vector<std::pair<std::string, std::vector<std::string>>> &getTopicOwnerVec();
    const std::map<std::string, uint32_t> &getVeticalGroupBrokerCountMap();
    std::string getTopicScdType() const;
    std::string getConfigStr();
    bool getDealErrorBroker() const;
    double getErrorBrokerDealRatio() const;
    std::string getReportZombieUrl() const;
    int64_t getZfsTimeout() const;
    int64_t getCommitDelayThreshold() const;
    int64_t getBrokerUnknownTimeout() const;
    uint32_t getDeadBrokerTimeoutSec() const;
    uint64_t getForbidCampaignTimeMs() const;
    bool getUseRecommendPort() const;
    uint64_t getChgPartcntIntervalUs() const;
    int32_t getMaxRestartCountInLocal() const;
    bool enableBackup() const;
    uint64_t getInitialMasterVersion() const;
    int64_t getForceSyncLeaderInfoInterval() const;
    std::string getMirrorZkRoot() const;
    const AuthorizerInfo &getAuthenticationConf() const;
    int64_t getLongPeriodIntervalSec() const;
    uint32_t getHeartbeatThreadNum() const;
    uint32_t getHeartbeatQueueSize() const;
    const std::vector<std::string> &getCleanAtDeleteTopicPatterns() const;
    int64_t getMetaInfoReplicateInterval() const;
    int64_t getMaxTopicAclSyncIntervalUs() const;

public:
    // for test
    void setZkRoot(const std::string &zkRoot_);
    void setWorkDir(const std::string &workDir);
    const std::string &getWorkDir();
    void setLogConfFile(const std::string &logConfFile);
    const std::string &getLogConfFile();

private:
    bool validate() const;

private:
    std::string userName;
    std::string serviceName;
    std::string hippoRoot;
    std::string zkRoot;
    std::string workDir;
    std::string logConfFile;
    std::string configVersion;
    uint16_t amonitorPort;
    uint32_t brokerCount;
    uint32_t adminCount;
    uint32_t dispatchIntervalMs;
    uint32_t candidateWaitTimeMs;
    uint32_t brokerNetLimitMB;
    std::string dfsRoot;
    std::string extendDfsRoot;
    std::string todelDfsRoot;
    std::string configPath;
    std::string backMetaPath;
    double cleanDataIntervalHour;
    double delExpiredTopicIntervalSec;
    double reserveDataHour;
    int queueSize;
    int threadNum;
    int ioThreadNum;
    int anetTimeoutLoopIntervalMs;
    bool exclusiveListenThread;
    bool promoteIoThreadPriority;
    int adminQueueSize;
    int adminThreadNum;
    int adminHttpQueueSize;
    int adminHttpThreadNum;
    int adminIoThreadNum;
    double decsionThreshold;
    int32_t leaderLeaseTime;
    double leaderLoopInterval;
    std::vector<std::string> syncAdminInfoPathVec;
    int64_t syncAdminInfoInterval;
    int64_t syncTopicInfoInterval;
    int64_t syncHeartbeatInterval;
    std::map<std::string, uint32_t> groupBrokerCountMap;
    std::map<std::string, uint32_t> veticalGroupBrokerCountMap;
    int64_t adjustResourceDuration;
    uint32_t exclusiveLevel;
    int64_t forceScheduleTimeSecond;
    int64_t releaseDeadWorkerTimeSecond;
    int64_t workerPartitionLimit;
    int64_t topicResourceLimit;
    int64_t minMaxPartitionBuffer;
    int64_t brokerCfgTTlSec;
    int recordLocalFile;
    uint32_t reserveBackMetaCount;
    uint32_t requestProcessTimeRatio; // 10-100
    uint32_t noUseTopicExpireTimeSec;
    uint32_t noUseTopicDeleteIntervalSec;
    uint32_t missTopicRecoverIntervalSec;
    uint32_t maxAllowNouseFileNum;
    bool updatePartResource;
    bool localMode;
    bool multiThreadDispatchTask;
    uint32_t dispatchTaskThreadNum;
    uint32_t heartbeatIntervalInUs;
    std::vector<std::pair<std::string, std::string>> topicGroupVec;
    std::vector<std::pair<std::string, std::vector<std::string>>> topicOwnerVec;
    bool fastRecover;
    std::string topicScdType;
    bool dealErrorBroker;
    double errorBrokerDealRatio;
    std::string reportZombieUrl;
    int64_t zfsTimeout;
    int64_t commitDelayThresholdInSec;
    int64_t brokerUnknownTimeout;
    uint32_t deadBrokerTimeoutSec;
    uint64_t forbidCampaignTimeMs;
    bool useRecommendPort;
    uint64_t chgPartcntIntervalUs;
    int32_t maxRestartCountInLocal;
    bool backup;
    uint64_t initialMasterVersion;
    int64_t forceSyncLeaderInfoInterval;
    std::string mirrorZkRoot;
    AuthorizerInfo authConf;
    int64_t longPeriodIntervalSec;
    uint32_t heartbeatThreadNum;
    uint32_t heartbeatQueueSize;
    std::vector<std::string> cleanAtDeleteTopicPatterns;
    int64_t metaInfoReplicateInterval;
    int64_t maxTopicAclSyncIntervalUs;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace config
} // namespace swift
