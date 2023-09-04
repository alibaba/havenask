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
#include <set>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/util/ZkDataAccessor.h"

namespace cm_basic {
class ZkWrapper;
} // namespace cm_basic

namespace swift {
namespace admin {

class AdminZkDataAccessor : public util::ZkDataAccessor {
public:
    AdminZkDataAccessor();
    virtual ~AdminZkDataAccessor();

private:
    AdminZkDataAccessor(const AdminZkDataAccessor &);
    AdminZkDataAccessor &operator=(const AdminZkDataAccessor &);

public:
    bool init(cm_basic::ZkWrapper *zkWrapper, const std::string &sysRoot) override;
    bool init(const std::string &sysRoot) override;
    void setRecordLocalFile(int flag) { _recordLocalFile = flag; }
    virtual bool setLeaderInfo(const protocol::LeaderInfo &leaderInfo);
    virtual bool getLeaderInfo(protocol::LeaderInfo &leaderInfo) const;

    virtual bool addLeaderInfoToHistory(const protocol::LeaderInfo &leaderInfo);
    virtual bool getHistoryLeaders(protocol::LeaderInfoHistory &historyLeaders) const;

    /*** new ways ***/
    protocol::TopicPartitionInfos getTopicPartitionInfos() const;

    virtual bool loadTopicInfos(protocol::TopicMetas &topicMetas, protocol::TopicPartitionInfos &topicPartInfos);
    virtual bool loadTopicSchemas();
    virtual bool addTopic(const protocol::TopicCreationRequest &topicMeta);
    virtual bool addTopics(const protocol::TopicBatchCreationRequest &topicMetas);
    virtual bool addPhysicTopic(const protocol::TopicCreationRequest &request,
                                int64_t brokerCfgTTlSec,
                                protocol::TopicCreationRequest &retLogicTopicMeta,
                                protocol::TopicCreationRequest &retLastPhysicTopicMeta,
                                protocol::TopicCreationRequest &retNewPhysicTopicMeta);
    virtual bool modifyTopic(const protocol::TopicCreationRequest &newMeta);
    virtual bool deleteTopic(const std::set<std::string> &names);
    bool deleteTopicsAllType(const std::map<std::string, protocol::TopicCreationRequest> &todelTopicMetas,
                             std::set<std::string> &deletedTopics,
                             std::vector<protocol::TopicCreationRequest> &updatedTopicMetas);
    virtual bool setTopicPartitionInfos(const protocol::TopicPartitionInfos &topicPartInfos);
    virtual void setTopicMetas(const protocol::TopicMetas &topicMetas);
    virtual bool writeTopicMetas(const protocol::TopicMetas &topicMetas);
    virtual bool writeTopicPartitionInfos(const protocol::TopicPartitionInfos &topicPartitionInfos);
    virtual bool readTopicMetas(protocol::TopicMetas &topicMetas);
    virtual bool readTopicMetas(std::string &content);
    virtual bool readTopicPartitionInfos(protocol::TopicPartitionInfos &topicPartitionInfos);
    virtual bool loadTopicInfosFromNewFiles(protocol::TopicMetas &topicMetas,
                                            protocol::TopicPartitionInfos &topicPartInfos);
    virtual bool setDispatchedTask(const protocol::DispatchInfo &dispatchInfo);
    virtual bool getDispatchedTask(const std::string &roleName, protocol::DispatchInfo &dispatchInfo);

    virtual bool readConfigVersion(std::string &versionStr);
    virtual bool writeConfigVersion(const std::string &versionStr);
    virtual bool readLeaderAddress(const std::string &roleName, std::string &leaderAddress);
    virtual bool recoverPartitionInfos(const protocol::TopicMetas &topicMetas,
                                       protocol::TopicPartitionInfos &partitionInfos);
    virtual bool setBrokerVersionInfos(const protocol::RoleVersionInfos &roleVersionInfos);
    virtual bool getBrokerVersionInfos(protocol::RoleVersionInfos &roleVersionInfos);
    virtual bool addTopicSchema(const std::string &topicName,
                                int32_t version,
                                const std::string &schema,
                                int32_t &removedVersion,
                                uint32_t maxAllowSchemaNum);
    virtual bool getSchema(const std::string &topicName, int32_t version, protocol::SchemaInfo &schemaInfo);
    virtual bool removeTopicSchema(const std::string &topicName);
    bool writeTopicRWTime(const protocol::TopicRWInfos &rwInfos);
    bool loadTopicRWTime(protocol::TopicRWInfos &rwInfos);
    bool loadLastDeletedNoUseTopics(protocol::TopicMetas &metas);
    bool loadDeletedNoUseTopics(const std::string &fileName, protocol::TopicMetas &metas);
    bool loadDeletedNoUseTopicFiles(std::vector<std::string> &topicFiles);
    bool loadChangePartCntTask();
    protocol::ErrorCode sendChangePartCntTask(const protocol::TopicCreationRequest &request);
    void updateChangePartCntTasks(const std::set<int64_t> &finishedIds);
    bool getLastDeletedNoUseTopicFileName(std::string &fileName);
    bool writeDeletedNoUseTopics(const protocol::TopicMetas &metas, uint32_t maxAllowNum = 10);
    const protocol::ChangeTopicPartCntTasks getChangePartCntTasks() const;
    bool getLogicTopicName(const std::string &physicName, std::string &logicName);
    bool writeMasterVersion(uint64_t targetVersion);
    bool readMasterVersion(uint64_t &masterVersion);
    bool serializeCleanAtDeleteTasks(const protocol::CleanAtDeleteTopicTasks &tasks);
    bool loadCleanAtDeleteTopicTasks(protocol::CleanAtDeleteTopicTasks &tasks);
    void removeOldWriterVersionData(const std::set<std::string> &topicNames);

private:
    bool mkZkDirs();
    virtual bool getTopicMeta(const std::string &topicName,
                              protocol::TopicCreationRequest &meta); // for test

    /*** old ways ***/
    virtual bool loadTopicInfosFromOldFiles(protocol::TopicMetas &topicMetas,
                                            protocol::TopicPartitionInfos &topicPartInfos);
    virtual bool getTopicMetaOld(const std::string &topicName, protocol::TopicCreationRequest &meta) const;
    virtual bool getTopicInfoOld(const std::string &topicName, protocol::TopicInfo &topicInfo) const;
    virtual bool getAllTopicNameOld(std::vector<std::string> &names) const;
    virtual bool writeLeaderAddress(const std::string &roleName, const std::string &leaderAddress);
    bool getOldestSchemaVersion(const protocol::SchemaInfos &scmInfos, int32_t &version, int &pos);

private:
    bool writeTopicSchema(const std::string &topicName, const protocol::SchemaInfos &scmInfos);
    void addTopicToMetas(const protocol::TopicCreationRequest &request,
                         protocol::TopicMetas &metas,
                         protocol::TopicPartitionInfos &partInfos);
    bool deleteTopic(const std::set<std::string> &toDeleteTopics,
                     const std::map<std::string, protocol::TopicCreationRequest> &toUpdateMetas,
                     std::set<std::string> &deletedTopics,
                     std::vector<protocol::TopicCreationRequest> &updatedMetas);
    void getUpdateAndDelTopics(const std::map<std::string, protocol::TopicCreationRequest> &todelTopicMetas,
                               std::map<std::string, protocol::TopicCreationRequest> &updateTopicMetas,
                               std::set<std::string> &deleteTopics);

private:
    std::string _zkPath;
    int _recordLocalFile;
    protocol::TopicMetas _topicMetas;
    protocol::TopicPartitionInfos _topicPartitionInfos;
    protocol::RoleVersionInfos _versionInfos;
    std::map<std::string, protocol::SchemaInfos> _topicSchemas;
    mutable autil::ReadWriteLock _changePartCntTasksLock;
    protocol::ChangeTopicPartCntTasks _changePartCntTasks;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(AdminZkDataAccessor);

} // namespace admin
} // namespace swift
