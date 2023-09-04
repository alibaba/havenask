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
#include <memory>
#include <stdint.h>
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/config/ClientAuthorizerInfo.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/version.h"

namespace swift {
namespace protocol {
class AllTopicInfoResponse;
class BrokerStatusRequest;
class BrokerStatusResponse;
class EmptyRequest;
class GetBrokerStatusRequest;
class GetBrokerStatusResponse;
class GetSchemaRequest;
class GetSchemaResponse;
class GetTopicRWTimeRequest;
class GetTopicRWTimeResponse;
class MasterInfoResponse;
class MissTopicRequest;
class MissTopicResponse;
class PartitionInfoResponse;
class RegisterSchemaRequest;
class RegisterSchemaResponse;
class TopicAclRequest;
class TopicAclResponse;
class TopicBatchCreationRequest;
class TopicBatchCreationResponse;
class TopicBatchDeletionRequest;
class TopicBatchDeletionResponse;
class TopicCreationRequest;
class TopicCreationResponse;
class TopicInfoResponse;
class TopicNameResponse;
} // namespace protocol

namespace network {

class SwiftAdminClient;

typedef std::shared_ptr<SwiftAdminClient> SwiftAdminClientPtr;

class SwiftRpcChannelManager;

typedef std::shared_ptr<SwiftRpcChannelManager> SwiftRpcChannelManagerPtr;

struct TopicInfoCacheItem {
    int64_t putTime = 0;
    protocol::TopicInfo topicInfo;
    bool fromLeader = false;
    bool hasError = false;
};

class SwiftAdminAdapter {
public:
    SwiftAdminAdapter(const std::string &zkRootPath,
                      const SwiftRpcChannelManagerPtr &channelManager,
                      bool useFollowerAdmin = false,
                      int64_t timeout = 3 * 1000,
                      int64_t refreshTime = 3 * 1000 * 1000);
    virtual ~SwiftAdminAdapter();

private:
    SwiftAdminAdapter(const SwiftAdminAdapter &);
    SwiftAdminAdapter &operator=(const SwiftAdminAdapter &);

public:
    virtual protocol::ErrorCode getBrokerAddress(const std::string &topicName,
                                                 uint32_t partitionId,
                                                 std::string &brokerAddress,
                                                 protocol::AuthenticationInfo authInfo);

    virtual protocol::ErrorCode getBrokerInfo(const std::string &topicName,
                                              uint32_t partitionId,
                                              std::string &brokerAddress,
                                              protocol::BrokerVersionInfo &versionInfo,
                                              protocol::AuthenticationInfo authInfo);

    virtual protocol::ErrorCode getTopicInfo(const std::string &topicName,
                                             protocol::TopicInfoResponse &response,
                                             int64_t topicVersion,
                                             protocol::AuthenticationInfo authInfo);

    virtual protocol::ErrorCode getPartitionCount(const std::string &topicName, uint32_t &partitionCount);

    virtual protocol::ErrorCode getPartitionCount(const std::string &topicName,
                                                  int64_t topicVersion,
                                                  uint32_t &partitionCount,
                                                  uint32_t &rangeCountInPartition);

    virtual protocol::ErrorCode
    getPartitionInfo(const std::string &topicName, uint32_t partitionId, protocol::PartitionInfoResponse &response);

    virtual protocol::ErrorCode getAllTopicInfo(protocol::AllTopicInfoResponse &response);

    virtual protocol::ErrorCode deleteTopic(const std::string &topicName);

    virtual protocol::ErrorCode createTopic(protocol::TopicCreationRequest &request);

    virtual protocol::ErrorCode deleteTopicBatch(protocol::TopicBatchDeletionRequest &request,
                                                 protocol::TopicBatchDeletionResponse &response);

    virtual protocol::ErrorCode createTopicBatch(protocol::TopicBatchCreationRequest &request,
                                                 protocol::TopicBatchCreationResponse &response);

    bool waitTopicReady(const std::string &topicName,
                        protocol::AuthenticationInfo authInfo,
                        int64_t timeout = 20 * 1000 * 1000);
    std::string getZkPath() const { return _zkRootPath; }

    virtual protocol::ErrorCode registerSchema(protocol::RegisterSchemaRequest &request,
                                               protocol::RegisterSchemaResponse &response);

    virtual protocol::ErrorCode getSchema(protocol::GetSchemaRequest &request,
                                          protocol::GetSchemaResponse &response,
                                          protocol::AuthenticationInfo authInfo);

    virtual protocol::ErrorCode reportBrokerStatus(protocol::BrokerStatusRequest &request,
                                                   protocol::BrokerStatusResponse &response);

    virtual protocol::ErrorCode getBrokerStatus(protocol::GetBrokerStatusRequest &request,
                                                protocol::GetBrokerStatusResponse &response);

    virtual protocol::ErrorCode getTopicRWTime(protocol::GetTopicRWTimeRequest &request,
                                               protocol::GetTopicRWTimeResponse &response);

    virtual protocol::ErrorCode getAllTopicName(protocol::TopicNameResponse &response);

    virtual protocol::ErrorCode reportMissTopic(protocol::MissTopicRequest &request,
                                                protocol::MissTopicResponse &response);

    virtual protocol::ErrorCode sealTopic(const std::string &topicName);
    virtual protocol::ErrorCode changePartCount(const std::string &topicName, uint32_t newPartCount);

    virtual protocol::ErrorCode updateWriterVersion(const protocol::TopicWriterVersionInfo &writerVersionInfo);
    void setLatelyErrorTimeMinIntervalUs(int64_t interval);
    void setLatelyErrorTimeMaxIntervalUs(int64_t interval);
    void setAuthenticationConf(const auth::ClientAuthorizerInfo &conf);
    auth::ClientAuthorizerInfo getAuthenticationConf();
    virtual protocol::ErrorCode modifyTopic(protocol::TopicCreationRequest &request,
                                            protocol::TopicCreationResponse &response);
    virtual protocol::ErrorCode getMasterInfo(protocol::EmptyRequest &request, protocol::MasterInfoResponse &response);
    virtual protocol::ErrorCode topicAclManage(protocol::TopicAclRequest &request,
                                               protocol::TopicAclResponse &response);

public:
    virtual protocol::ErrorCode createAdminClient();
    void addErrorInfoToCacheTopic(const std::string &topicName, uint32_t partitionId, const std::string &address);

private:
    bool needResetAdminClient(protocol::ErrorCode ec);
    void resetAdminClient();
    protocol::ErrorCode doGetBrokerInfo(const std::string &topicName,
                                        uint32_t partitionId,
                                        std::string &brokerAddress,
                                        protocol::BrokerVersionInfo &versionInfo,
                                        protocol::AuthenticationInfo authInfo);
    protocol::ErrorCode doGetAllTopicInfo(protocol::AllTopicInfoResponse &response);
    protocol::ErrorCode doGetTopicInfo(const std::string &topicName,
                                       protocol::TopicInfoResponse &response,
                                       bool fromLeader,
                                       protocol::AuthenticationInfo authInfo);
    protocol::ErrorCode
    doGetPartitionInfo(const std::string &topicName, uint32_t partitionId, protocol::PartitionInfoResponse &response);
    protocol::ErrorCode doDeleteTopic(const std::string &topicName);
    protocol::ErrorCode doCreateTopic(protocol::TopicCreationRequest &request);
    protocol::ErrorCode doUpdateWriterVersion(const protocol::TopicWriterVersionInfo &writerVersionInfo);

    protocol::ErrorCode getLatelyError(int64_t now);
    void checkError(protocol::ErrorCode ec, int64_t now);
    bool isGeneralError(protocol::ErrorCode ec);
    int64_t getLatelyErrorInterval();
    bool getTopicInfoFromCache(const std::string &topicName, protocol::TopicInfo &topicInfo, bool &fromLeader);
    void putTopicInfoIntoCache(const std::string &topicName, const protocol::TopicInfo &topicInfo, bool fromLeader);
    protocol::ErrorCode getTopicInfoWithLeader(const std::string &topicName,
                                               protocol::TopicInfoResponse &response,
                                               bool fromLeader,
                                               protocol::AuthenticationInfo authInfo);
    protocol::ErrorCode getTopicInfoOpt(const std::string &topicName,
                                        protocol::TopicInfoResponse &response,
                                        int64_t topicVersion,
                                        protocol::AuthenticationInfo authInfo);
    template <typename T>
    void addRequestExtendInfo(T &request);
    template <typename T>
    void addRequestExtendInfo(T &request, const protocol::AuthenticationInfo &authInfo);

protected:
    const std::string _zkRootPath;
    bool _useFollowerAdmin;
    autil::ThreadMutex _mutex;
    SwiftRpcChannelManagerPtr _channelManager;
    SwiftAdminClientPtr _adminClient;
    SwiftAdminClientPtr _readAdminClient;
    protocol::ErrorCode _lastGeneralErrorCode;
    int64_t _lastGeneralErrorTime;
    int64_t _timeout;
    int64_t _refreshTime;
    typedef std::map<std::string, TopicInfoCacheItem> TopicInfoCache;
    TopicInfoCache _topicInfoCache;
    autil::ThreadMutex _cacheMutex;
    int64_t _latelyErrorTimeMinInterval;
    int64_t _latelyErrorTimeMaxInterval;
    auth::ClientAuthorizerInfo _authConf;

private:
    friend class SwiftAdminAdapterTest;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
void SwiftAdminAdapter::addRequestExtendInfo(T &request) {
    protocol::ClientVersionInfo *versionInfo = request.mutable_versioninfo();
    versionInfo->set_version(swift_admin_version_str);
    versionInfo->set_supportauthentication(true);
    if (!_authConf.isEmpty()) {
        protocol::AuthenticationInfo *authenticationInfo = request.mutable_authentication();
        authenticationInfo->set_username(_authConf.username);
        authenticationInfo->set_passwd(_authConf.passwd);
    }
}

template <typename T>
void SwiftAdminAdapter::addRequestExtendInfo(T &request, const protocol::AuthenticationInfo &authInfo) {
    addRequestExtendInfo(request);
    if (!authInfo.username().empty() && !authInfo.passwd().empty()) {
        *request.mutable_authentication() = authInfo;
    } else {
        *(request.mutable_authentication()->mutable_accessauthinfo()) = authInfo.accessauthinfo();
    }
}

SWIFT_TYPEDEF_PTR(SwiftAdminAdapter);

} // namespace network
} // namespace swift
