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
#include "swift/config/AdminConfig.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <memory>
#include <ostream>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/fs/FileSystem.h"
#include "swift/config/AuthorizerParser.h"
#include "swift/config/ConfigDefine.h"
#include "swift/config/ConfigReader.h"

namespace swift {
namespace config {
AUTIL_LOG_SETUP(swift, AdminConfig);

using namespace std;
using namespace autil;
using namespace swift::config;

AdminConfig::AdminConfig()
    : amonitorPort(DEFAULT_AMONITOR_PORT)
    , brokerCount(0)
    , adminCount(0)
    , dispatchIntervalMs(1 * 1000)
    , candidateWaitTimeMs(1000)
    , brokerNetLimitMB(1000)
    , cleanDataIntervalHour(1)
    , delExpiredTopicIntervalSec(300)
    , reserveDataHour(24 * 365)
    , queueSize(DEFAULT_QUEUE_SIZE)
    , threadNum(DEFAULT_THREAD_NUM)
    , ioThreadNum(DEFAULT_IO_THREAD_NUM)
    , anetTimeoutLoopIntervalMs(0)
    , exclusiveListenThread(false)
    , promoteIoThreadPriority(false)
    , adminQueueSize(DEFAULT_QUEUE_SIZE)
    , adminThreadNum(DEFAULT_THREAD_NUM)
    , adminHttpQueueSize(DEFAULT_QUEUE_SIZE)
    , adminHttpThreadNum(DEFAULT_THREAD_NUM)
    , adminIoThreadNum(DEFAULT_IO_THREAD_NUM)
    , decsionThreshold(DEFAULT_DECSION_THRESHOLD)
    , leaderLeaseTime(DEFAULT_LEADER_LEASE_TIME)
    , leaderLoopInterval(DEFAULT_LEADER_LOOP_INTERVAL)
    , syncAdminInfoInterval(DEFAULT_SYNC_ADMIN_INFO_INTERVAL)
    , syncTopicInfoInterval(DEFAULT_SYNC_TOPIC_INFO_INTERVAL)
    , syncHeartbeatInterval(DEFAULT_SYNC_HEARTBEAT_INTERVAL)
    , adjustResourceDuration(DEFAULT_ADJUST_RESOURCE_DURATION)
    , exclusiveLevel(0)
    , forceScheduleTimeSecond(-1)
    , releaseDeadWorkerTimeSecond(DEFAULT_RELEASE_DEAD_WORKER_TIME)
    , workerPartitionLimit(DEFAULT_WORKER_PARTITION_LIMIT)
    , topicResourceLimit(DEFAULT_TOPIC_RESOURCE_LIMIT)
    , minMaxPartitionBuffer(DEFAULT_MIN_MAX_PARTITION_BUFFER)
    , brokerCfgTTlSec(DEFAULT_OBSOLETE_FILE_TIME_INTERVAL)
    , recordLocalFile(-1)
    , reserveBackMetaCount(200)
    , requestProcessTimeRatio(DEFAULT_ADMIN_PROCESS_REQUEST_TIME_RATIO)
    , noUseTopicExpireTimeSec(0)
    , noUseTopicDeleteIntervalSec(0)
    , missTopicRecoverIntervalSec(0)
    , maxAllowNouseFileNum(10)
    , updatePartResource(false)
    , localMode(false)
    , multiThreadDispatchTask(false)
    , dispatchTaskThreadNum(10)
    , heartbeatIntervalInUs(0)
    , fastRecover(false)
    , dealErrorBroker(false)
    , errorBrokerDealRatio(DEFAULT_ADMIN_ERROR_BROKER_DEAL_RATIO)
    , zfsTimeout(DEFAULT_ADMIN_ZFS_TIMEOUT)
    , commitDelayThresholdInSec(DEFAULT_ADMIN_COMMIT_DELAY_THRESHOLD)
    , brokerUnknownTimeout(DEFAULT_BROKER_UNKNOWN_TIMEOUT)
    , deadBrokerTimeoutSec(180)
    , forbidCampaignTimeMs(10000)
    , useRecommendPort(true)
    , chgPartcntIntervalUs(2000000)
    , maxRestartCountInLocal(-1)
    , backup(false)
    , initialMasterVersion(0)
    , forceSyncLeaderInfoInterval(DEFAULT_ADMIN_FORCE_SYNC_INTERVAL)
    , longPeriodIntervalSec(3600 * 6)
    , heartbeatThreadNum(DEFAULT_HEARTBEAT_THREAD_NUM)
    , heartbeatQueueSize(DEFAULT_HEARTBEAT_QUEUE_SIZE)
    , metaInfoReplicateInterval(DEFAULT_META_INFO_REPLICATOR_INTERVAL)
    , maxTopicAclSyncIntervalUs(DEFAULT_MAX_TOPIC_ACL_SYNC_INTERVAL_US) {}

AdminConfig::~AdminConfig() {}
string AdminConfig::getApplicationId() const { return userName + "_" + serviceName; }
string AdminConfig::getUserName() const { return userName; }
string AdminConfig::getServiceName() const { return serviceName; }
string AdminConfig::getHippoRoot() const { return hippoRoot; }
string AdminConfig::getZkRoot() const { return zkRoot; }
uint16_t AdminConfig::getAmonitorPort() const { return amonitorPort; }
uint32_t AdminConfig::getBrokerCount() const { return brokerCount; }
uint32_t AdminConfig::getAdminCount() const { return adminCount; }
uint32_t AdminConfig::getDispatchIntervalMs() const { return dispatchIntervalMs; }
uint32_t AdminConfig::getCandidateWaitTimeMs() const { return candidateWaitTimeMs; };
uint32_t AdminConfig::getBrokerNetLimitMB() const { return brokerNetLimitMB; }
double AdminConfig::getCleanDataIntervalHour() const { return cleanDataIntervalHour; }
double AdminConfig::getDelExpiredTopicIntervalSec() const { return delExpiredTopicIntervalSec; }
double AdminConfig::getReserveDataByHour() const { return reserveDataHour; }
string AdminConfig::getDfsRoot() const { return dfsRoot; }
string AdminConfig::getExtendDfsRoot() const { return extendDfsRoot; }
string AdminConfig::getTodelDfsRoot() const { return todelDfsRoot; }
string AdminConfig::getConfigPath() const { return configPath; }
string AdminConfig::getBackMetaPath() const { return backMetaPath; }
int AdminConfig::getQueueSize() const { return queueSize; }
int AdminConfig::getThreadNum() const { return threadNum; }
int AdminConfig::getIoThreadNum() const { return ioThreadNum; }
int AdminConfig::getAdminHttpQueueSize() const { return adminHttpQueueSize; }
int AdminConfig::getAdminHttpThreadNum() const { return adminHttpThreadNum; }
int AdminConfig::getAdminQueueSize() const { return adminQueueSize; }
int AdminConfig::getAdminThreadNum() const { return adminThreadNum; }
int AdminConfig::getAdminIoThreadNum() const { return adminIoThreadNum; }
int AdminConfig::getAnetTimeoutLoopIntervalMs() const { return anetTimeoutLoopIntervalMs; }
bool AdminConfig::getExclusiveListenThread() const { return exclusiveListenThread; }
bool AdminConfig::getPromoteIoThreadPriority() const { return promoteIoThreadPriority; }
string AdminConfig::getConfigVersion() const { return configVersion; }
double AdminConfig::getDecsionThreshold() const { return decsionThreshold; }
void AdminConfig::setZkRoot(const string &zkRoot_) { zkRoot = zkRoot_; }
void AdminConfig::setWorkDir(const std::string &workDir_) { workDir = workDir_; }
const std::string &AdminConfig::getWorkDir() { return workDir; }
void AdminConfig::setLogConfFile(const std::string &logConfFile_) { logConfFile = logConfFile_; }
const std::string &AdminConfig::getLogConfFile() { return logConfFile; }

int32_t AdminConfig::getLeaderLeaseTime() const { return leaderLeaseTime; }
double AdminConfig::getLeaderLoopInterval() const { return leaderLoopInterval; }
const vector<string> &AdminConfig::getSyncAdminInfoPath() { return syncAdminInfoPathVec; }
int64_t AdminConfig::getSyncAdminInfoInterval() { return syncAdminInfoInterval; }
int64_t AdminConfig::getSyncTopicInfoInterval() { return syncTopicInfoInterval; }
int64_t AdminConfig::getSyncHeartbeatInterval() { return syncHeartbeatInterval; }
int64_t AdminConfig::getAdjustResourceDuration() { return adjustResourceDuration; }
const map<string, uint32_t> &AdminConfig::getGroupBrokerCountMap() { return groupBrokerCountMap; }
BrokerExclusiveLevel AdminConfig::getExclusiveLevel() const {
    if (exclusiveLevel >= BROKER_EL_UNKNOWN) {
        return BROKER_EL_ALL;
    } else {
        return (BrokerExclusiveLevel)exclusiveLevel;
    }
}
int64_t AdminConfig::getForceScheduleTimeSecond() const { return forceScheduleTimeSecond; }
int AdminConfig::getRecordLocalFile() const { return recordLocalFile; }
uint32_t AdminConfig::getReserveBackMetaCount() const { return reserveBackMetaCount; }
int64_t AdminConfig::getReleaseDeadWorkerTimeSecond() const { return releaseDeadWorkerTimeSecond; }
int64_t AdminConfig::getWorkPartitionLimit() const { return workerPartitionLimit; }
int64_t AdminConfig::getTopicResourceLimit() const { return topicResourceLimit; }
int64_t AdminConfig::getMinMaxPartitionBufferSizeMb() const { return minMaxPartitionBuffer; }
int64_t AdminConfig::getBrokerCfgTTlSec() const { return brokerCfgTTlSec; }
uint32_t AdminConfig::getRequestProcessTimeRatio() const { return requestProcessTimeRatio; }
uint32_t AdminConfig::getNoUseTopicExpireTimeSec() const { return noUseTopicExpireTimeSec; }
uint32_t AdminConfig::getNoUseTopicDeleteIntervalSec() const { return noUseTopicDeleteIntervalSec; }
uint32_t AdminConfig::getMissTopicRecoverIntervalSec() const { return missTopicRecoverIntervalSec; }
uint32_t AdminConfig::getMaxAllowNouseFileNum() const { return maxAllowNouseFileNum; }
bool AdminConfig::autoUpdatePartResource() const { return updatePartResource; }
bool AdminConfig::isLocalMode() const { return localMode; }
bool AdminConfig::enableMultiThreadDispatchTask() const { return multiThreadDispatchTask; }
uint32_t AdminConfig::getDispatchTaskThreadNum() const { return dispatchTaskThreadNum; }
bool AdminConfig::enableFastRecover() const { return fastRecover; }
uint32_t AdminConfig::getHeartbeatIntervalInUs() const { return heartbeatIntervalInUs; }

string AdminConfig::getTopicScdType() const { return topicScdType; }
const std::vector<std::pair<std::string, std::string>> &AdminConfig::getTopicGroupVec() { return topicGroupVec; }
const vector<pair<string, vector<string>>> &AdminConfig::getTopicOwnerVec() { return topicOwnerVec; }
const map<string, uint32_t> &AdminConfig::getVeticalGroupBrokerCountMap() { return veticalGroupBrokerCountMap; }
bool AdminConfig::getDealErrorBroker() const { return dealErrorBroker; }
double AdminConfig::getErrorBrokerDealRatio() const { return errorBrokerDealRatio; }
string AdminConfig::getReportZombieUrl() const { return reportZombieUrl; }
int64_t AdminConfig::getZfsTimeout() const { return zfsTimeout; }
int64_t AdminConfig::getCommitDelayThreshold() const { return commitDelayThresholdInSec; }
int64_t AdminConfig::getBrokerUnknownTimeout() const { return brokerUnknownTimeout; }
uint32_t AdminConfig::getDeadBrokerTimeoutSec() const { return deadBrokerTimeoutSec; }
uint64_t AdminConfig::getForbidCampaignTimeMs() const { return forbidCampaignTimeMs; }
bool AdminConfig::getUseRecommendPort() const { return useRecommendPort; }
uint64_t AdminConfig::getChgPartcntIntervalUs() const { return chgPartcntIntervalUs; }
int32_t AdminConfig::getMaxRestartCountInLocal() const { return maxRestartCountInLocal; }
bool AdminConfig::enableBackup() const { return backup; }
uint64_t AdminConfig::getInitialMasterVersion() const { return initialMasterVersion; }
int64_t AdminConfig::getForceSyncLeaderInfoInterval() const { return forceSyncLeaderInfoInterval; }
string AdminConfig::getMirrorZkRoot() const { return mirrorZkRoot; }
const AuthorizerInfo &AdminConfig::getAuthenticationConf() const { return authConf; }
int64_t AdminConfig::getLongPeriodIntervalSec() const { return longPeriodIntervalSec; }
uint32_t AdminConfig::getHeartbeatThreadNum() const { return heartbeatThreadNum; }
uint32_t AdminConfig::getHeartbeatQueueSize() const { return heartbeatQueueSize; }
const vector<string> &AdminConfig::getCleanAtDeleteTopicPatterns() const { return cleanAtDeleteTopicPatterns; }
int64_t AdminConfig::getMetaInfoReplicateInterval() const { return metaInfoReplicateInterval; }
int64_t AdminConfig::getMaxTopicAclSyncIntervalUs() const { return maxTopicAclSyncIntervalUs; }

string AdminConfig::parseConfigVersion(const string &configPath) {
    vector<string> configStrVec = StringUtil::split(configPath, "/");
    if (configStrVec.size() != 0) {
        return configStrVec[configStrVec.size() - 1];
    } else {
        AUTIL_LOG(WARN, "config path is empty, can't get config version");
        return "";
    }
}

AdminConfig *AdminConfig::loadConfig(const string &configPath) {
    string configFile = fslib::fs::FileSystem::joinFilePath(configPath, SWIFT_CONF_FILE_NAME);
    ConfigReader reader;
    if (!reader.read(configFile)) {
        AUTIL_LOG(ERROR, "Failed to read %s.", configFile.c_str());
        return NULL;
    }
    if (!reader.hasSection(SECTION_COMMON)) {
        AUTIL_LOG(ERROR, "Failed to find section[%s].", SECTION_COMMON);
        return NULL;
    }
    if (!reader.hasSection(SECTION_ADMIN)) {
        AUTIL_LOG(ERROR, "Failed to find section[%s].", SECTION_ADMIN);
        return NULL;
    }
    if (!reader.hasSection(SECTION_BROKER)) {
        AUTIL_LOG(ERROR, "Failed to find section[%s].", SECTION_BROKER);
        return NULL;
    }

    unique_ptr<AdminConfig> adminConfig(new AdminConfig);
    adminConfig->configPath = configPath;
    adminConfig->configVersion = parseConfigVersion(configPath);

#define GET_ADMIN_CONFIG_VALUE(type, name, value, need, sectionName)                                                   \
    {                                                                                                                  \
        type v;                                                                                                        \
        if (reader.getOption(sectionName, name, v)) {                                                                  \
            adminConfig->value = v;                                                                                    \
        } else if (need) {                                                                                             \
            AUTIL_LOG(ERROR, "Invalid config: [%s] is needed", name);                                                  \
            return NULL;                                                                                               \
        }                                                                                                              \
    }

    GET_ADMIN_CONFIG_VALUE(uint16_t, AMONITOR_PORT, amonitorPort, false, SECTION_COMMON);
    GET_ADMIN_CONFIG_VALUE(string, USER_NAME, userName, true, SECTION_COMMON);
    GET_ADMIN_CONFIG_VALUE(string, SERVICE_NAME, serviceName, true, SECTION_COMMON);
    GET_ADMIN_CONFIG_VALUE(string, HIPPO_ROOT_PATH, hippoRoot, true, SECTION_COMMON);
    GET_ADMIN_CONFIG_VALUE(string, ZOOKEEPER_ROOT_PATH, zkRoot, true, SECTION_COMMON);
    GET_ADMIN_CONFIG_VALUE(uint32_t, BROKER_COUNT, brokerCount, true, SECTION_COMMON);
    GET_ADMIN_CONFIG_VALUE(uint32_t, ADMIN_COUNT, adminCount, true, SECTION_COMMON);
    GET_ADMIN_CONFIG_VALUE(uint32_t, BROKER_EXCLUSIVE_LEVEL, exclusiveLevel, false, SECTION_COMMON);
    GET_ADMIN_CONFIG_VALUE(int32_t, LEADER_LEASE_TIME, leaderLeaseTime, false, SECTION_COMMON);
    GET_ADMIN_CONFIG_VALUE(double, LEADER_LOOP_INTERVAL, leaderLoopInterval, false, SECTION_COMMON);
    GET_ADMIN_CONFIG_VALUE(bool, USE_RECOMMEND_PORT, useRecommendPort, false, SECTION_COMMON);
    GET_ADMIN_CONFIG_VALUE(bool, IS_LOCAL_MODE, localMode, false, SECTION_COMMON);
    GET_ADMIN_CONFIG_VALUE(bool, ENABLE_BACKUP, backup, false, SECTION_COMMON);
    GET_ADMIN_CONFIG_VALUE(uint64_t, INITIAL_MASTER_VERSION, initialMasterVersion, false, SECTION_COMMON);
    GET_ADMIN_CONFIG_VALUE(string, MIRROR_ZKROOT, mirrorZkRoot, false, SECTION_COMMON);
    GET_ADMIN_CONFIG_VALUE(int64_t, MAX_TOPIC_ACL_SYNC_INTERVAL_US, maxTopicAclSyncIntervalUs, false, SECTION_COMMON);

    GET_ADMIN_CONFIG_VALUE(uint32_t, DISPATCH_INTERVAL, dispatchIntervalMs, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(uint32_t, CANDIDATE_WAIT_TIME_MS, candidateWaitTimeMs, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(uint32_t, BROKER_NETLIMIT_MB, brokerNetLimitMB, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(double, CLEAN_DATA_INTERVAL_HOUR, cleanDataIntervalHour, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(
        double, DELETE_EXPIRED_TOPIC_INTERVAL_SECOND, delExpiredTopicIntervalSec, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(double, RESERVE_DATA_HOUR, reserveDataHour, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(double, DECSION_THRESHOLD, decsionThreshold, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int64_t, ADJUST_RESOURCE_DURATION_MS, adjustResourceDuration, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int64_t, FORCE_SCHEDULE_TIME, forceScheduleTimeSecond, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int64_t, RELEASE_DEAD_WORKER_TIME, releaseDeadWorkerTimeSecond, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int64_t, TOPIC_RESOURCE_LIMIT, topicResourceLimit, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int64_t, WORKER_PARTITION_LIMIT, workerPartitionLimit, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int64_t, SYNC_ADMIN_INFO_INTERVAL, syncAdminInfoInterval, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int64_t, SYNC_TOPIC_INFO_INTERVAL, syncTopicInfoInterval, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int64_t, SYNC_HEARTBEAT_INTERVAL, syncHeartbeatInterval, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int32_t, MAX_RESTART_COUNT_IN_LOCAL, maxRestartCountInLocal, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int, ADMIN_THREAD_QUEUE_SIZE, adminQueueSize, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int, ADMIN_THREAD_NUM, adminThreadNum, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int, ADMIN_HTTP_THREAD_QUEUE_SIZE, adminHttpQueueSize, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int, ADMIN_HTTP_THREAD_NUM, adminHttpThreadNum, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int64_t, MIN_MAX_PARTITION_BUFFER_SIZE_LIMIT, minMaxPartitionBuffer, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int64_t, FORCE_SYNC_LEADER_INFO_INTERVAL, forceSyncLeaderInfoInterval, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int64_t, OBSOLETE_FILE_TIME_INTERVAL_HOUR, brokerCfgTTlSec, false, SECTION_BROKER);
    if (DEFAULT_OBSOLETE_FILE_TIME_INTERVAL != adminConfig->brokerCfgTTlSec) {
        adminConfig->brokerCfgTTlSec *= 3600;
    }
    GET_ADMIN_CONFIG_VALUE(int64_t, OBSOLETE_FILE_TIME_INTERVAL_SEC, brokerCfgTTlSec, false, SECTION_BROKER);
    GET_ADMIN_CONFIG_VALUE(uint32_t, ADMIN_REQUEST_PROCESS_TIME_RATIO, requestProcessTimeRatio, false, SECTION_ADMIN);

    GET_ADMIN_CONFIG_VALUE(int, ADMIN_IO_THREAD_NUM, adminIoThreadNum, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int, RECORD_LOCAL_FILE, recordLocalFile, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(uint32_t, RESERVE_BACK_META_COUNT, reserveBackMetaCount, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(string, BACKUP_META_PATH, backMetaPath, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(bool, AUTO_UPDATE_PART_RESOURCE, updatePartResource, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(bool, ENABLE_MULTI_TRHEAD_DISPATCH_TASK, multiThreadDispatchTask, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(uint32_t, DISPATCH_TASK_THREAD_NUM, dispatchTaskThreadNum, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(uint32_t, NOUSE_TOPIC_EXPIRE_TIME_SEC, noUseTopicExpireTimeSec, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(
        uint32_t, NOUSE_TOPIC_DELETE_INTERVAL_SEC, noUseTopicDeleteIntervalSec, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(
        uint32_t, MISS_TOPIC_RECOVER_INTERVAL_SEC, missTopicRecoverIntervalSec, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(uint32_t, MAX_ALLOW_NOUSE_FILE_NUM, maxAllowNouseFileNum, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(string, TOPIC_SCHEDULE_TYPE, topicScdType, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(bool, DEAL_ERROR_BROKER, dealErrorBroker, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(double, ERROR_BROKER_DEAL_RATIO, errorBrokerDealRatio, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(string, REPORT_ZOMBIE_URL, reportZombieUrl, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int64_t, CS_ZFS_TIMEOUT, zfsTimeout, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int64_t, CS_COMMIT_DELAY_THRESHOLD_SEC, commitDelayThresholdInSec, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int64_t, BROKER_UNKNOWN_TIMEOUT, brokerUnknownTimeout, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(uint32_t, DEAD_BROKER_TIMEOUT_SEC, deadBrokerTimeoutSec, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(uint64_t, FORBID_CAMPAIGN_TIME_MS, forbidCampaignTimeMs, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(uint64_t, CHG_TOPIC_PARTCNT_INTERVAL_US, chgPartcntIntervalUs, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int64_t, LONG_PERIOD_INTERVAL_SEC, longPeriodIntervalSec, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(uint32_t, HEARTBEAT_THREAD_NUM, heartbeatThreadNum, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(uint32_t, HEARTBEAT_QUEUE_SIZE, heartbeatQueueSize, false, SECTION_ADMIN);
    GET_ADMIN_CONFIG_VALUE(int64_t, META_INFO_REPLICATE_INTERVAL, metaInfoReplicateInterval, false, SECTION_ADMIN);

    GET_ADMIN_CONFIG_VALUE(string, DFS_ROOT_PATH, dfsRoot, true, SECTION_BROKER);
    GET_ADMIN_CONFIG_VALUE(string, EXTEND_DFS_ROOT_PATH, extendDfsRoot, false, SECTION_BROKER);
    GET_ADMIN_CONFIG_VALUE(string, TODEL_DFS_ROOT_PATH, todelDfsRoot, false, SECTION_BROKER);
    GET_ADMIN_CONFIG_VALUE(int, THREAD_QUEUE_SIZE, queueSize, false, SECTION_BROKER);
    GET_ADMIN_CONFIG_VALUE(int, THREAD_NUM, threadNum, false, SECTION_BROKER);
    GET_ADMIN_CONFIG_VALUE(int, IO_THREAD_NUM, ioThreadNum, false, SECTION_BROKER);
    GET_ADMIN_CONFIG_VALUE(int, ANET_TIMEOUT_LOOP_INTERVAL_MS, anetTimeoutLoopIntervalMs, false, SECTION_BROKER);
    GET_ADMIN_CONFIG_VALUE(bool, EXCLUSIVE_LISTEN_THREAD, exclusiveListenThread, false, SECTION_BROKER);
    GET_ADMIN_CONFIG_VALUE(bool, PROMOTE_IO_THREAD_PRIORITY, promoteIoThreadPriority, false, SECTION_BROKER);
    GET_ADMIN_CONFIG_VALUE(bool, ENABLE_FAST_RECOVER, fastRecover, false, SECTION_BROKER);
    GET_ADMIN_CONFIG_VALUE(int, HEARTBEAT_INTERVAL_IN_US, heartbeatIntervalInUs, false, SECTION_BROKER);

    string CADTopicPatterns;
    if (reader.getOption(SECTION_ADMIN, CLEAN_AT_DELETE_TOPIC_PATTERNS, CADTopicPatterns)) {
        adminConfig->cleanAtDeleteTopicPatterns = StringUtil::split(CADTopicPatterns, ";");
    }
    string syncPath;
    if (reader.getOption(SECTION_ADMIN, SYNC_ADMIN_INFO_PATH, syncPath)) {
        adminConfig->syncAdminInfoPathVec = StringUtil::split(syncPath, ";");
    }
    string groupInfo;
    if (reader.getOption(SECTION_COMMON, GROUP_BROKER_COUNT, groupInfo)) {
        const vector<string> &groupVec = StringUtil::split(groupInfo, ";");
        for (size_t i = 0; i < groupVec.size(); i++) {
            const vector<string> &infoVec = StringUtil::split(groupVec[i], ":");
            if (infoVec.size() == 2) {
                auto count = StringUtil::fromString<uint32_t>(infoVec[1]);
                adminConfig->groupBrokerCountMap[infoVec[0]] = count;
                adminConfig->veticalGroupBrokerCountMap[infoVec[0]] = count;
            }
        }
    }
    string groupTopicInfo;
    if (reader.getOption(SECTION_ADMIN, TOPIC_GROUP_VEC, groupTopicInfo)) {
        const vector<string> &groupVec = StringUtil::split(groupTopicInfo, ";");
        for (size_t i = 0; i < groupVec.size(); i++) {
            const vector<string> &infoVec = StringUtil::split(groupVec[i], ":");
            if (infoVec.size() == 2) {
                adminConfig->topicGroupVec.emplace_back(infoVec[0], infoVec[1]);
            }
        }
    }
    string topicOwnerInfo;
    if (reader.getOption(SECTION_ADMIN, TOPIC_OWNER_VEC, topicOwnerInfo)) {
        const vector<string> &ownerVec = StringUtil::split(topicOwnerInfo, ";");
        for (size_t i = 0; i < ownerVec.size(); i++) {
            const vector<string> &infoVec = StringUtil::split(ownerVec[i], ":");
            if (infoVec.size() == 2) {
                const vector<string> &owners = StringUtil::split(infoVec[1], ",");
                adminConfig->topicOwnerVec.emplace_back(infoVec[0], owners);
            }
        }
    }
    adminConfig->groupBrokerCountMap[DEFAULT_GROUP_NAME] = adminConfig->brokerCount;
    adminConfig->veticalGroupBrokerCountMap[DEFAULT_GROUP_NAME] = adminConfig->brokerCount;
    adminConfig->brokerCount = 0;
    map<string, uint32_t>::iterator iter = adminConfig->groupBrokerCountMap.begin();
    for (; iter != adminConfig->groupBrokerCountMap.end(); iter++) {
        adminConfig->brokerCount += iter->second;
    }
    string veticalBrokerCntInfo;
    if (reader.getOption(SECTION_ADMIN, VETICAL_BROKERCNT_VEC, veticalBrokerCntInfo)) {
        const vector<string> &vbrokerVec = StringUtil::split(veticalBrokerCntInfo, ";");
        for (size_t i = 0; i < vbrokerVec.size(); i++) {
            const vector<string> &cntVec = StringUtil::split(vbrokerVec[i], ":");
            uint32_t count = 0;
            if (cntVec.size() == 2 && StringUtil::strToUInt32(cntVec[1].c_str(), count)) {
                adminConfig->veticalGroupBrokerCountMap[cntVec[0]] = count;
            }
        }
    }
    AuthorizerParser authorizerParser;
    if (!authorizerParser.parseAuthorizer(reader, adminConfig->authConf)) {
        AUTIL_LOG(ERROR, "lack of necessary internal authentication");
        return NULL;
    }
    if (!adminConfig->validate()) {
        AUTIL_LOG(WARN, "swift config validate failed!");
        return NULL;
    }
    return adminConfig.release();
}

bool AdminConfig::validate() const {
    if (userName.empty()) {
        AUTIL_LOG(ERROR, "UserName can't be empty.");
        return false;
    }
    if (serviceName.empty()) {
        AUTIL_LOG(ERROR, "ServiceName can't be empty.");
        return false;
    }
    if (!localMode && hippoRoot.empty()) {
        AUTIL_LOG(ERROR, "HippoRoot can't be empty.");
        return false;
    }
    if (zkRoot.empty()) {
        AUTIL_LOG(ERROR, "Zookeeper can't be empty.");
        return false;
    }
    if (brokerCount <= 0) {
        AUTIL_LOG(ERROR, "Broker count must greater than 0.");
        return false;
    }
    if (adminCount <= 0) {
        AUTIL_LOG(ERROR, "Admin count must greater than 0.");
        return false;
    }
    if (decsionThreshold > 1) {
        AUTIL_LOG(ERROR, "Decsion threshold can't greater than 1.");
        return false;
    }
    if (!dfsRoot.empty() && dfsRoot == backMetaPath) {
        AUTIL_LOG(ERROR, "backMetaPath should not same as dfsRoot");
        return false;
    }
    if (reserveBackMetaCount < 10) {
        AUTIL_LOG(ERROR, "reserveBackMetaCount should bigger than 10");
        return false;
    }
    if (requestProcessTimeRatio < 10 || requestProcessTimeRatio > 100) {
        AUTIL_LOG(ERROR, "requestProcessTimeRatio should between 10 and 100");
        return false;
    }
    if (brokerNetLimitMB < 10) {
        AUTIL_LOG(ERROR, "brokerNetLimitMB should larger than 10");
        return false;
    }
    if ("vetical" == topicScdType) {
        for (const auto &group : groupBrokerCountMap) {
            const auto iter = veticalGroupBrokerCountMap.find(group.first);
            if (iter != veticalGroupBrokerCountMap.end() && (iter->second > group.second || 0 == iter->second)) {
                AUTIL_LOG(ERROR,
                          "group[%s] vetical broker count[%u] invalid, "
                          "should > 0 and  <= total[%u]",
                          group.first.c_str(),
                          iter->second,
                          group.second);
                return false;
            }
        }
    }
    if (backup) {
        if (!fastRecover) {
            AUTIL_LOG(ERROR, "fast recover should enabled when enable backup");
            return false;
        }
        if (mirrorZkRoot.empty()) {
            AUTIL_LOG(ERROR, "mirror zk root should config when enable backup");
            return false;
        }
        if (1 != syncAdminInfoPathVec.size()) {
            AUTIL_LOG(ERROR, "sync admin info should config only one when enable backup");
            return false;
        }
    }
    if (cleanAtDeleteTopicPatterns.size() != 0) {
        for (int i = 0; i < cleanAtDeleteTopicPatterns.size(); i++) {
            if (cleanAtDeleteTopicPatterns[i] == "") {
                AUTIL_LOG(ERROR, "clean at delete topic pattern should not be empty");
                return false;
            }
        }
    }
    return true;
}

std::string AdminConfig::getConfigStr() {
    std::ostringstream oss;
    oss << boolalpha << "userName:" << userName << " serviceName:" << serviceName << " hippoRoot:" << hippoRoot
        << " zkRoot:" << zkRoot << " configVersion:" << configVersion << " amonitorPort:" << amonitorPort
        << " brokerCount:" << brokerCount << " adminCount:" << adminCount
        << " dispatchIntervalMs:" << dispatchIntervalMs << " candidateWaitTimeMs:" << candidateWaitTimeMs
        << " brokerNetLimitMB:" << brokerNetLimitMB << " dfsRoot:" << dfsRoot << " extendDfsRoot:" << extendDfsRoot
        << " todelDfsRoot:" << todelDfsRoot << " configPath:" << configPath << " backMetaPath:" << backMetaPath
        << " cleanDataIntervalHour:" << cleanDataIntervalHour
        << " delExpiredTopicIntervalSec:" << delExpiredTopicIntervalSec << " reserveDataHour:" << reserveDataHour
        << " queueSize:" << queueSize << " threadNum:" << threadNum << " ioThreadNum:" << ioThreadNum
        << " anetTimeoutLoopIntervalMs:" << anetTimeoutLoopIntervalMs << " adminQueueSize:" << adminQueueSize
        << " adminThreadNum:" << adminThreadNum << " adminHttpQueueSize:" << adminHttpQueueSize
        << " adminHttpThreadNum:" << adminHttpThreadNum << " adminIoThreadNum:" << adminIoThreadNum
        << " decsionThreshold:" << decsionThreshold << " leaderLeaseTime:" << leaderLeaseTime
        << " leaderLoopInterval:" << leaderLoopInterval << " syncAdminInfoPathVec:[";
    for (auto &s : syncAdminInfoPathVec) {
        oss << s << " ";
    }
    oss << "] syncAdminInfoInterval:" << syncAdminInfoInterval << " syncTopicInfoInterval:" << syncTopicInfoInterval
        << " syncHeartbeatInterval:" << syncHeartbeatInterval << " groupBrokerCountMap:[";
    for (auto &item : groupBrokerCountMap) {
        oss << item.first << ":" << item.second << " ";
    }
    oss << "] veticalGroupBrokerCountMap:[";
    for (auto &item : veticalGroupBrokerCountMap) {
        oss << item.first << ":" << item.second << " ";
    }
    oss << "] adjustResourceDuration:" << adjustResourceDuration << " exclusiveLevel:" << exclusiveLevel
        << " forceScheduleTimeSecond:" << forceScheduleTimeSecond
        << " releaseDeadWorkerTimeSecond:" << releaseDeadWorkerTimeSecond
        << " workerPartitionLimit:" << workerPartitionLimit << " topicResourceLimit:" << topicResourceLimit
        << " minMaxPartitionBuffer:" << minMaxPartitionBuffer << " brokerCfgTTlSec:" << brokerCfgTTlSec
        << " recordLocalFile:" << recordLocalFile << " reserveBackMetaCount:" << reserveBackMetaCount
        << " requestProcessTimeRatio:" << requestProcessTimeRatio
        << " noUseTopicExpireTimeSec:" << noUseTopicExpireTimeSec
        << " noUseTopicDeleteIntervalSec:" << noUseTopicDeleteIntervalSec
        << " missTopicRecoverIntervalSec:" << missTopicRecoverIntervalSec
        << " maxAllowNouseFileNum:" << maxAllowNouseFileNum << " autoUpdatePartResource:" << updatePartResource
        << " localMode:" << localMode << " workDir:" << workDir << " logConfFile:" << logConfFile
        << " multiThreadDispatchTask:" << multiThreadDispatchTask << " dispatchTaskThreadNum:" << dispatchTaskThreadNum
        << " heartbeatIntervalInUs:" << heartbeatIntervalInUs << " topicScdType:" << topicScdType
        << " dealErrorBroker:" << dealErrorBroker << " errorBrokerDealRatio:" << errorBrokerDealRatio
        << " reportZombieUrl:" << reportZombieUrl << " zfsTimeout:" << zfsTimeout
        << " commitDelayThreshold:" << commitDelayThresholdInSec << " brokerUnknownTimeout:" << brokerUnknownTimeout
        << " deadBrokerTimeoutSec:" << deadBrokerTimeoutSec << " forbidCampaignTimeMs:" << forbidCampaignTimeMs
        << " useRecommendPort:" << useRecommendPort << " chgPartcntIntervalUs:" << chgPartcntIntervalUs
        << " maxRestartCountInLocal:" << maxRestartCountInLocal << " backup:" << backup
        << " initialMasterVersion:" << initialMasterVersion
        << " forceSyncLeaderInfoInterval:" << forceSyncLeaderInfoInterval << " mirrorZkRoot:" << mirrorZkRoot
        << " longPeriodIntervalSec:" << longPeriodIntervalSec << " heartbeatThreadNum:" << heartbeatThreadNum
        << " heartbeatQueueSize:" << heartbeatQueueSize << " maxTopicAclSyncIntervalUs:" << maxTopicAclSyncIntervalUs
        << " enableAuthentication:" << authConf.getEnable() << " authentications:[";
    map<string, string> authentications = authConf.getSysUsers();
    for (auto &item : authentications) {
        oss << item.first << ":" << item.second << " ";
    }
    oss << "] topicGroupVec:[";
    for (auto &item : topicGroupVec) {
        oss << item.first << ":" << item.second << " ";
    }
    oss << "] topicOwnerVec:[";
    for (auto &item : topicOwnerVec) {
        oss << item.first << ":(";
        for (auto &owner : item.second) {
            oss << owner << " ";
        }
        oss << ") ";
    }
    oss << "] cleanAtDeleteTopicPatterns:[";
    for (auto &item : cleanAtDeleteTopicPatterns) {
        oss << item << " ";
    }
    oss << "]";
    oss << " metaInfoReplicateInterval:" << metaInfoReplicateInterval;
    return oss.str();
}

} // namespace config
} // namespace swift
