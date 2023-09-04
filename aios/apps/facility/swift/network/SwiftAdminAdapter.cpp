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
#include "swift/network/SwiftAdminAdapter.h"

#include <algorithm>
#include <assert.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/stubs/port.h>
#include <iosfwd>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "autil/TimeUtility.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include "swift/common/PathDefine.h"
#include "swift/network/ClientFileUtil.h"
#include "swift/network/SwiftAdminClient.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/Heartbeat.pb.h"

using namespace std;
using namespace autil;
using namespace google::protobuf;
using namespace google::protobuf::io;

using namespace swift::protocol;
using namespace swift::common;
namespace swift {
namespace network {
AUTIL_LOG_SETUP(swift, SwiftAdminAdapter);

static const int64_t LATELY_ERROR_TIME_MIN_INTERVAL = 300 * 1000;   // 0.3 s
static const int64_t LATELY_ERROR_TIME_MAX_INTERVAL = 10000 * 1000; // 10 s

SwiftAdminAdapter::SwiftAdminAdapter(const string &zkRootPath,
                                     const SwiftRpcChannelManagerPtr &channelManager,
                                     bool useFollowerAdmin,
                                     int64_t timeout,
                                     int64_t refreshTime)
    : _zkRootPath(zkRootPath)
    , _useFollowerAdmin(useFollowerAdmin)
    , _channelManager(channelManager)
    , _lastGeneralErrorCode(ERROR_NONE)
    , _lastGeneralErrorTime(-1)
    , _timeout(timeout)
    , _refreshTime(refreshTime) {
    _latelyErrorTimeMaxInterval = LATELY_ERROR_TIME_MAX_INTERVAL;
    _latelyErrorTimeMinInterval = LATELY_ERROR_TIME_MIN_INTERVAL;
}

SwiftAdminAdapter::~SwiftAdminAdapter() { resetAdminClient(); }

#define CHECK_LATELY_ERROR                                                                                             \
    int64_t now = TimeUtility::currentTime();                                                                          \
    ErrorCode ec = ERROR_NONE;                                                                                         \
    ec = getLatelyError(now);                                                                                          \
    if (ERROR_NONE != ec) {                                                                                            \
        AUTIL_LOG(WARN, "has laterly error [%s]!", ErrorCode_Name(ec).c_str());                                        \
        return ec;                                                                                                     \
    }

#define DO_ADMIN_COMMON_RPC(adminClient, METHOD, response)                                                             \
    {                                                                                                                  \
        if (!adminClient->METHOD(&request, &response)) {                                                               \
            AUTIL_LOG(WARN, "swift admin arpc failed.");                                                               \
            ec = ERROR_CLIENT_ARPC_ERROR;                                                                              \
        }                                                                                                              \
                                                                                                                       \
        if (response.errorinfo().errcode() != ERROR_NONE) {                                                            \
            AUTIL_LOG(WARN,                                                                                            \
                      "swift admin error, errorcode:[%d], errormsg[%s].",                                              \
                      response.errorinfo().errcode(),                                                                  \
                      response.errorinfo().errmsg().c_str());                                                          \
            ec = response.errorinfo().errcode();                                                                       \
        }                                                                                                              \
    }

ErrorCode SwiftAdminAdapter::getBrokerAddress(const string &topicName,
                                              uint32_t partitionId,
                                              string &brokerAddress,
                                              protocol::AuthenticationInfo authInfo) {
    protocol::BrokerVersionInfo versionInfo;
    return getBrokerInfo(topicName, partitionId, brokerAddress, versionInfo, authInfo);
}

ErrorCode SwiftAdminAdapter::getBrokerInfo(const std::string &topicName,
                                           uint32_t partitionId,
                                           std::string &brokerAddress,
                                           BrokerVersionInfo &versionInfo,
                                           protocol::AuthenticationInfo authInfo) {
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_readAdminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }
    ec = doGetBrokerInfo(topicName, partitionId, brokerAddress, versionInfo, authInfo);
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    if (ec != ERROR_NONE) {
        addErrorInfoToCacheTopic(topicName, partitionId, "unkonwn");
    }
    return ec;
}

ErrorCode SwiftAdminAdapter::getLatelyError(int64_t now) {
    if ((ERROR_NONE == _lastGeneralErrorCode) || (_lastGeneralErrorTime + getLatelyErrorInterval() < now)) {
        return ERROR_NONE;
    }

    return _lastGeneralErrorCode;
}

int64_t SwiftAdminAdapter::getLatelyErrorInterval() {
    srand((uint64_t)time(NULL));
    int64_t interval =
        _latelyErrorTimeMinInterval + (rand() % (_latelyErrorTimeMaxInterval - _latelyErrorTimeMinInterval + 1));
    AUTIL_LOG(WARN, "lately error interval is [%ld]", interval);
    return interval;
}

void SwiftAdminAdapter::checkError(ErrorCode ec, int64_t now) {
    if (isGeneralError(ec)) {
        _lastGeneralErrorCode = ec;
        _lastGeneralErrorTime = now;
    } else {
        _lastGeneralErrorCode = ERROR_NONE;
        _lastGeneralErrorTime = -1;
    }
}

bool SwiftAdminAdapter::isGeneralError(ErrorCode ec) {
    return (ERROR_CLIENT_RPC_CONNECT == ec || ERROR_CLIENT_GET_ADMIN_INFO_FAILED == ec ||
            ERROR_CLIENT_SYS_STOPPED == ec || ERROR_CLIENT_ARPC_ERROR == ec);
}

ErrorCode SwiftAdminAdapter::getAllTopicInfo(AllTopicInfoResponse &response) {
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_readAdminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }
    ec = doGetAllTopicInfo(response);
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    return ec;
}

void SwiftAdminAdapter::resetAdminClient() {
    _adminClient.reset();
    _readAdminClient.reset();
}

ErrorCode
SwiftAdminAdapter::getPartitionInfo(const string &topicName, uint32_t partitionId, PartitionInfoResponse &response) {
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_readAdminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }

    ec = doGetPartitionInfo(topicName, partitionId, response);
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    return ec;
}

ErrorCode SwiftAdminAdapter::getTopicInfo(const string &topicName,
                                          TopicInfoResponse &response,
                                          int64_t topicVersion,
                                          protocol::AuthenticationInfo authInfo) {
    return getTopicInfoOpt(topicName, response, topicVersion, authInfo);
}

ErrorCode SwiftAdminAdapter::getTopicInfoWithLeader(const string &topicName,
                                                    TopicInfoResponse &response,
                                                    bool fromLeader,
                                                    protocol::AuthenticationInfo authInfo) {
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_readAdminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }

    ec = doGetTopicInfo(topicName, response, fromLeader, authInfo);
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    return ec;
}

ErrorCode SwiftAdminAdapter::getPartitionCount(const string &topicName, uint32_t &partitionCount) {
    uint32_t rangeCountInPartition = 0;
    return getPartitionCount(topicName, 0, partitionCount, rangeCountInPartition);
}

ErrorCode SwiftAdminAdapter::getPartitionCount(const string &topicName,
                                               int64_t topicVersion,
                                               uint32_t &partitionCount,
                                               uint32_t &rangeCountInPartition) {
    TopicInfoResponse response;
    ErrorCode ec = getTopicInfoOpt(topicName, response, topicVersion, {});
    if (ERROR_NONE == ec) {
        partitionCount = (uint32_t)response.topicinfo().partitioncount();
        rangeCountInPartition = (uint32_t)response.topicinfo().rangecountinpartition();
    }
    return ec;
}

ErrorCode SwiftAdminAdapter::getTopicInfoOpt(const string &topicName,
                                             TopicInfoResponse &response,
                                             int64_t topicVersion,
                                             protocol::AuthenticationInfo authInfo) {
    TopicInfo cacheTopicInfo;
    bool fromLeader = false;
    bool cacheHit = getTopicInfoFromCache(topicName, cacheTopicInfo, fromLeader);
    if (cacheHit) {
        if (!cacheTopicInfo.has_modifytime() || cacheTopicInfo.modifytime() < topicVersion) // version is modifytime
        {
            AUTIL_LOG(INFO,
                      "hit cache topic version[%ld] less than client "
                      "version[%ld], now getTopicInfo from admin leader",
                      cacheTopicInfo.modifytime(),
                      topicVersion);
            TopicInfoResponse tmpResponse;
            ErrorCode ec = getTopicInfoWithLeader(topicName, tmpResponse, true, authInfo);
            if (ec != ERROR_NONE) {
                return ec;
            }
            if (!tmpResponse.has_topicinfo()) {
                AUTIL_LOG(WARN, "get topic[%s] info from leader failed, topic info empty", topicName.c_str());
                return ERROR_CLIENT_INVALID_RESPONSE;
            }
            response = tmpResponse;
        } else {
            AUTIL_LOG(
                INFO, "hit cache topic version[%ld], client version[%ld]", cacheTopicInfo.modifytime(), topicVersion);
            TopicInfo *ti = response.mutable_topicinfo();
            *ti = cacheTopicInfo;
            ErrorInfo *ei = response.mutable_errorinfo();
            ei->set_errcode(ERROR_NONE);
            ei->set_errmsg(ErrorCode_Name(ei->errcode()));
        }
    } else {
        TopicInfoResponse tmpResponse;
        ErrorCode ec = getTopicInfoWithLeader(topicName, tmpResponse, false, authInfo);
        if (ec != ERROR_NONE) {
            return ec;
        }
        if (!tmpResponse.has_topicinfo()) {
            AUTIL_LOG(WARN, "get topic[%s] info from follow failed, topic info empty", topicName.c_str());
            return ERROR_CLIENT_INVALID_RESPONSE;
        }
        if (tmpResponse.topicinfo().has_modifytime() && tmpResponse.topicinfo().modifytime() >= topicVersion &&
            topicVersion > 0) {
            AUTIL_LOG(INFO,
                      "follow admin topic version[%ld], client version[%ld]",
                      tmpResponse.topicinfo().modifytime(),
                      topicVersion);
            response = tmpResponse;
        } else {
            TopicInfoResponse tmpResponse;
            ErrorCode ec = getTopicInfoWithLeader(topicName, tmpResponse, true, authInfo);
            if (ec != ERROR_NONE) {
                return ec;
            }
            if (!tmpResponse.has_topicinfo()) {
                AUTIL_LOG(WARN, "get topic[%s] info from leader failed, topic info empty", topicName.c_str());
                return ERROR_CLIENT_INVALID_RESPONSE;
            }
            AUTIL_LOG(INFO,
                      "leader admin topic version[%ld], client version[%ld]",
                      tmpResponse.topicinfo().modifytime(),
                      topicVersion);
            response = tmpResponse;
        }
    }
    return ERROR_NONE;
}

ErrorCode SwiftAdminAdapter::createTopic(protocol::TopicCreationRequest &request) {
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_adminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }
    ec = doCreateTopic(request);
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    return ec;
}

ErrorCode SwiftAdminAdapter::createTopicBatch(TopicBatchCreationRequest &request,
                                              TopicBatchCreationResponse &response) {
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_adminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }
    addRequestExtendInfo(request);
    DO_ADMIN_COMMON_RPC(_adminClient, createTopicBatch, response);
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    return ec;
}

bool SwiftAdminAdapter::waitTopicReady(const string &topicName,
                                       protocol::AuthenticationInfo authInfo,
                                       int64_t timeout) {
    int64_t endTime = TimeUtility::currentTime() + timeout;
    while (TimeUtility::currentTime() < endTime) {
        TopicInfoResponse response;
        ErrorCode ec = getTopicInfo(topicName, response, 0, authInfo);
        if (ec == ERROR_NONE && response.topicinfo().status() == TOPIC_STATUS_RUNNING) {
            return true;
        } else {
            usleep(500 * 1000);
        }
    }
    return false;
}

ErrorCode SwiftAdminAdapter::deleteTopic(const std::string &topicName) {
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_adminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }

    ec = doDeleteTopic(topicName);
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    return ec;
}

ErrorCode SwiftAdminAdapter::deleteTopicBatch(TopicBatchDeletionRequest &request,
                                              TopicBatchDeletionResponse &response) {
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_adminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }
    addRequestExtendInfo(request);
    DO_ADMIN_COMMON_RPC(_adminClient, deleteTopicBatch, response);
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    return ec;
}

ErrorCode SwiftAdminAdapter::doCreateTopic(protocol::TopicCreationRequest &request) {
    TopicCreationResponse response;
    ErrorCode ec = ERROR_NONE;
    addRequestExtendInfo(request);
    DO_ADMIN_COMMON_RPC(_adminClient, createTopic, response);
    return ec;
}

ErrorCode SwiftAdminAdapter::doDeleteTopic(const std::string &topicName) {
    TopicDeletionRequest request;
    request.set_topicname(topicName);
    addRequestExtendInfo(request);
    TopicDeletionResponse response;
    ErrorCode ec = ERROR_NONE;
    DO_ADMIN_COMMON_RPC(_adminClient, deleteTopic, response);
    return ec;
}

ErrorCode SwiftAdminAdapter::createAdminClient() {
    string leaderInfoFile = common::PathDefine::getLeaderInfoFile(_zkRootPath);
    string leaderInfoContent;
    ClientFileUtil fileUtil;
    if (!fileUtil.readFile(leaderInfoFile, leaderInfoContent) || leaderInfoContent.empty()) {
        AUTIL_LOG(WARN, "get leader info content failed. zookeeper path[%s]", leaderInfoFile.c_str());
        return ERROR_CLIENT_GET_ADMIN_INFO_FAILED;
    }

    LeaderInfo leaderInfo;
    ArrayInputStream stream((void *)leaderInfoContent.data(), leaderInfoContent.size());
    if (!leaderInfo.ParseFromZeroCopyStream(&stream)) {
        AUTIL_LOG(WARN, "parse leader info failed!");
        return ERROR_CLIENT_GET_ADMIN_INFO_FAILED;
    }
    if (leaderInfo.address().empty()) {
        AUTIL_LOG(WARN, "admin leader address is empty!");
        return ERROR_CLIENT_GET_ADMIN_INFO_FAILED;
    }
    AUTIL_LOG(INFO, "leader info [%s]", leaderInfo.ShortDebugString().c_str());
    if (leaderInfo.sysstop()) {
        AUTIL_LOG(WARN, "swift has stopped!");
        return ERROR_CLIENT_SYS_STOPPED;
    }
    _adminClient.reset(new SwiftAdminClient(leaderInfo.address(), _channelManager, _timeout));
    _readAdminClient = _adminClient;
    AUTIL_LOG(INFO, "create admin client to [%s] success!", leaderInfo.address().c_str());
    if (_useFollowerAdmin) {
        vector<string> adminAddrVec;
        for (int i = 0; i < leaderInfo.admininfos_size(); i++) {
            AdminInfo *adminInfo = leaderInfo.mutable_admininfos(i);
            if (adminInfo->isalive() && !adminInfo->isprimary()) {
                adminAddrVec.push_back(adminInfo->address());
            }
        }
        if (adminAddrVec.size() != 0) {
            int64_t curTime = TimeUtility::currentTime();
            srand(curTime);
            size_t i = rand() % adminAddrVec.size();
            AUTIL_LOG(INFO, "create admin client to [%s] success!", adminAddrVec[i].c_str());
            _readAdminClient.reset(new SwiftAdminClient(adminAddrVec[i], _channelManager, _timeout));
        }
    }
    return ERROR_NONE;
}

bool SwiftAdminAdapter::needResetAdminClient(ErrorCode ec) {
    return ec == ERROR_CLIENT_ARPC_ERROR || ec == ERROR_ADMIN_NOT_LEADER || ec == ERROR_ADMIN_IP_SET_INVALID;
}

ErrorCode SwiftAdminAdapter::doGetBrokerInfo(const string &topicName,
                                             uint32_t partitionId,
                                             string &brokerAddress,
                                             BrokerVersionInfo &versionInfo,
                                             protocol::AuthenticationInfo authInfo) {
    TopicInfo cacheTopicInfo;
    bool fromLeader;
    bool cacheHit = getTopicInfoFromCache(topicName, cacheTopicInfo, fromLeader);
    if (cacheHit) {
        AUTIL_LOG(INFO, "[%s %d] hit topic info cache, fromLeader[%d]", topicName.c_str(), partitionId, fromLeader);
    } else {
        AUTIL_LOG(INFO, "[%s %d] not hit topic info cache, now rpc follow admin", topicName.c_str(), partitionId);
        TopicInfoResponse response;
        ErrorCode ec = doGetTopicInfo(topicName, response, false, authInfo);
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "get topic info[%s] failed, error[%s]", topicName.c_str(), ErrorCode_Name(ec).c_str());
            return ec;
        }
        cacheTopicInfo = response.topicinfo();
    }
    if (TOPIC_TYPE_LOGIC == cacheTopicInfo.topictype()) {
        AUTIL_LOG(WARN,
                  "[%s %d] is logic topic, "
                  "return ERROR_CLIENT_LOGIC_TOPIC_NOT_IN_BROKER",
                  topicName.c_str(),
                  partitionId);
        return ERROR_CLIENT_LOGIC_TOPIC_NOT_IN_BROKER;
    }
    if (cacheTopicInfo.partitioninfos_size() <= (int32_t)partitionId) {
        AUTIL_LOG(WARN,
                  "cacheTopicInfo partition size[%d] < partitionId[%d], error["
                  "ERROR_CLIENT_GET_BROKER_ADDRESS_FAILED]",
                  cacheTopicInfo.partitioninfos_size(),
                  partitionId);
        return ERROR_CLIENT_GET_BROKER_ADDRESS_FAILED;
    }
    const PartitionInfo &partInfo = cacheTopicInfo.partitioninfos(partitionId);
    if (!partInfo.has_brokeraddress() || partInfo.brokeraddress().empty()) {
        AUTIL_LOG(WARN,
                  "[%s %d] partition not be scheduled, error["
                  "ERROR_CLIENT_GET_BROKER_ADDRESS_FAILED]",
                  topicName.c_str(),
                  partitionId);
        return ERROR_CLIENT_GET_BROKER_ADDRESS_FAILED;
    }
    brokerAddress = partInfo.brokeraddress();
    versionInfo = partInfo.versioninfo();
    return ERROR_NONE;
}

ErrorCode
SwiftAdminAdapter::doGetPartitionInfo(const string &topicName, uint32_t partitionId, PartitionInfoResponse &response) {
    assert(_readAdminClient);
    PartitionInfoRequest request;
    request.set_topicname(topicName);
    request.add_partitionids(partitionId);
    addRequestExtendInfo(request);
    int64_t begTime = TimeUtility::currentTime();
    if (!_readAdminClient->getPartitionInfo(&request, &response)) {
        AUTIL_LOG(WARN, "swift admin arpc failed.");
        return ERROR_CLIENT_ARPC_ERROR;
    }
    int64_t endTime = TimeUtility::currentTime();
    AUTIL_LOG(INFO,
              "real get partition [%s %d] info used [%ldus], content: [%s]",
              topicName.c_str(),
              (int)partitionId,
              endTime - begTime,
              response.ShortDebugString().c_str());
    if (response.errorinfo().errcode() != ERROR_NONE) {
        AUTIL_LOG(WARN,
                  "swift admin error, errorcode:[%d], errormsg[%s].",
                  response.errorinfo().errcode(),
                  response.errorinfo().errmsg().c_str());
        return response.errorinfo().errcode();
    }

    return ERROR_NONE;
}

ErrorCode SwiftAdminAdapter::doGetTopicInfo(const string &topicName,
                                            TopicInfoResponse &response,
                                            bool fromLeader,
                                            protocol::AuthenticationInfo authInfo) {
    assert(_adminClient);
    assert(_readAdminClient);
    TopicInfoRequest request;
    request.set_topicname(topicName);
    addRequestExtendInfo(request, authInfo);
    int64_t begTime = TimeUtility::currentTime();
    if (fromLeader) {
        if (!_adminClient->getTopicInfo(&request, &response)) {
            AUTIL_LOG(WARN, "swift admin arpc failed");
            return ERROR_CLIENT_ARPC_ERROR;
        }
    } else {
        if (!_readAdminClient->getTopicInfo(&request, &response)) {
            AUTIL_LOG(WARN, "swift admin arpc failed");
            return ERROR_CLIENT_ARPC_ERROR;
        }
    }
    int64_t endTime = TimeUtility::currentTime();
    AUTIL_LOG(INFO,
              "real get topic[%s] info used [%ld us], from [%d]",
              topicName.c_str(),
              endTime - begTime,
              int(fromLeader));
    if (response.errorinfo().errcode() != ERROR_NONE) {
        AUTIL_LOG(WARN,
                  "swift admin error, errorcode:[%s], errormsg[%s].",
                  ErrorCode_Name(response.errorinfo().errcode()).c_str(),
                  response.errorinfo().errmsg().c_str());
        return response.errorinfo().errcode();
    }
    if (response.has_topicinfo()) {
        putTopicInfoIntoCache(topicName, response.topicinfo(), fromLeader);
    }
    return ERROR_NONE;
}

ErrorCode SwiftAdminAdapter::doGetAllTopicInfo(AllTopicInfoResponse &response) {
    assert(_readAdminClient);
    EmptyRequest emptyRequest;
    addRequestExtendInfo(emptyRequest);
    int64_t begTime = TimeUtility::currentTime();
    if (!_readAdminClient->getAllTopicInfo(&emptyRequest, &response)) {
        AUTIL_LOG(WARN, "swift admin arpc failed.");
        return ERROR_CLIENT_ARPC_ERROR;
    }
    int64_t endTime = TimeUtility::currentTime();
    AUTIL_LOG(INFO, "real get all topic info used [%ld]", endTime - begTime);
    if (response.errorinfo().errcode() != ERROR_NONE) {
        AUTIL_LOG(WARN,
                  "swift admin error, errorcode:[%d], errormsg[%s].",
                  response.errorinfo().errcode(),
                  response.errorinfo().errmsg().c_str());
        return response.errorinfo().errcode();
    }

    return ERROR_NONE;
}

bool SwiftAdminAdapter::getTopicInfoFromCache(const string &topicName,
                                              protocol::TopicInfo &topicInfo,
                                              bool &fromLeader) {
    ScopedLock lock(_cacheMutex);
    TopicInfoCache::iterator iter = _topicInfoCache.find(topicName);
    if (iter == _topicInfoCache.end()) {
        return false;
    }
    int64_t cacheTime = TimeUtility::currentTime() - iter->second.putTime;
    // need cache topic info even has error.
    if (cacheTime >= _refreshTime ||
        (iter->second.hasError && cacheTime > min(_refreshTime, _latelyErrorTimeMinInterval))) {
        AUTIL_LOG(INFO,
                  "need refresh [%s] clear topic info from cache , cacheTime [%ld], put time [%ld], error [%d], "
                  "refresh time [%ld] ",
                  topicName.c_str(),
                  cacheTime,
                  iter->second.putTime,
                  iter->second.hasError,
                  _refreshTime);
        _topicInfoCache.erase(iter);
        return false;
    } else {
        topicInfo = iter->second.topicInfo;
        fromLeader = iter->second.fromLeader;
        return true;
    }
}
void SwiftAdminAdapter::putTopicInfoIntoCache(const string &topicName,
                                              const protocol::TopicInfo &topicInfo,
                                              bool fromLeader) {
    ScopedLock lock(_cacheMutex);
    TopicInfoCacheItem cacheItem;
    cacheItem.putTime = TimeUtility::currentTime();
    cacheItem.topicInfo = topicInfo;
    cacheItem.fromLeader = fromLeader;
    _topicInfoCache[topicName] = cacheItem;
    AUTIL_LOG(INFO, "[%s] cache topic info, put time [%ld] ", topicName.c_str(), cacheItem.putTime);
}

void SwiftAdminAdapter::addErrorInfoToCacheTopic(const std::string &topicName,
                                                 uint32_t partitionId,
                                                 const std::string &address) {
    ScopedLock lock(_cacheMutex);
    auto iter = _topicInfoCache.find(topicName);
    if (iter != _topicInfoCache.end() && !iter->second.hasError) {
        if (partitionId >= iter->second.topicInfo.partitioninfos_size()) {
            iter->second.hasError = true;
            AUTIL_LOG(INFO,
                      "[%s %u] add error into topic info, put time [%ld], cur topic part count [%d]",
                      topicName.c_str(),
                      partitionId,
                      iter->second.putTime,
                      iter->second.topicInfo.partitioninfos_size());
            return;
        }
        const PartitionInfo &partInfo = iter->second.topicInfo.partitioninfos(partitionId);
        size_t pos = address.find(":");
        string rawAddress = pos != string::npos ? address.substr(pos + 1) : address;
        if (rawAddress == partInfo.brokeraddress() || partInfo.brokeraddress().empty() || rawAddress == "unkonwn") {
            iter->second.hasError = true;
            AUTIL_LOG(INFO,
                      "[%s %u] add error into topic info, put time [%ld]",
                      topicName.c_str(),
                      partitionId,
                      iter->second.putTime);
        } else {
            AUTIL_LOG(INFO,
                      "[%s %u]add error, but try reuse for broker address has change from [%s] to [%s]",
                      topicName.c_str(),
                      partitionId,
                      rawAddress.c_str(),
                      partInfo.brokeraddress().c_str());
        }
    }
}

ErrorCode SwiftAdminAdapter::registerSchema(RegisterSchemaRequest &request,
                                            protocol::RegisterSchemaResponse &response) {
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_adminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }
    addRequestExtendInfo(request);
    DO_ADMIN_COMMON_RPC(_adminClient, registerSchema, response);
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    return ec;
}

ErrorCode SwiftAdminAdapter::getSchema(GetSchemaRequest &request,
                                       GetSchemaResponse &response,
                                       protocol::AuthenticationInfo authInfo) {
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_readAdminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }
    addRequestExtendInfo(request, authInfo);
    DO_ADMIN_COMMON_RPC(_readAdminClient, getSchema, response);
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    return ec;
}

ErrorCode SwiftAdminAdapter::reportBrokerStatus(BrokerStatusRequest &request, BrokerStatusResponse &response) {
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_adminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }
    addRequestExtendInfo(request);
    DO_ADMIN_COMMON_RPC(_adminClient, reportBrokerStatus, response);
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    return ec;
}

ErrorCode SwiftAdminAdapter::getBrokerStatus(GetBrokerStatusRequest &request, GetBrokerStatusResponse &response) {
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_adminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }
    addRequestExtendInfo(request);
    DO_ADMIN_COMMON_RPC(_adminClient, getBrokerStatus, response);
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    return ec;
}

ErrorCode SwiftAdminAdapter::getTopicRWTime(GetTopicRWTimeRequest &request, GetTopicRWTimeResponse &response) {
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_readAdminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }
    addRequestExtendInfo(request);
    DO_ADMIN_COMMON_RPC(_readAdminClient, getTopicRWTime, response);
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    return ec;
}

ErrorCode SwiftAdminAdapter::getAllTopicName(TopicNameResponse &response) {
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_readAdminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }
    EmptyRequest request;
    addRequestExtendInfo(request);
    DO_ADMIN_COMMON_RPC(_readAdminClient, getAllTopicName, response);
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    return ec;
}

// for follow admin
ErrorCode SwiftAdminAdapter::reportMissTopic(MissTopicRequest &request, MissTopicResponse &response) {
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_adminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }
    addRequestExtendInfo(request);
    DO_ADMIN_COMMON_RPC(_adminClient, reportMissTopic, response);
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    return ec;
}

ErrorCode SwiftAdminAdapter::sealTopic(const string &topicName) {
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_adminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }
    TopicCreationRequest request;
    TopicCreationResponse response;
    request.set_topicname(topicName);
    request.set_sealed(true);
    addRequestExtendInfo(request);
    DO_ADMIN_COMMON_RPC(_adminClient, modifyTopic, response);
    if (ERROR_NONE != ec) {
        AUTIL_LOG(
            ERROR, "seal topic[%s] fail, ec[%d], ret[%s]", topicName.c_str(), ec, response.ShortDebugString().c_str());
    } else {
        AUTIL_LOG(INFO, "seal topic[%s] success", topicName.c_str());
    }
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    return ec;
}

ErrorCode SwiftAdminAdapter::changePartCount(const string &topicName, uint32_t newPartCount) {
    if (0 == newPartCount) {
        AUTIL_LOG(ERROR, "newPartCount:0 is invalid");
        return ERROR_CLIENT_INVALID_PARAMETERS;
    }
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_adminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }
    TopicCreationRequest request;
    TopicCreationResponse response;
    request.set_topicname(topicName);
    request.set_partitioncount(newPartCount);
    addRequestExtendInfo(request);
    DO_ADMIN_COMMON_RPC(_adminClient, modifyTopic, response);
    if (ERROR_NONE != ec) {
        AUTIL_LOG(ERROR,
                  "change topic[%s] part count fail, ec[%d], ret[%s]",
                  topicName.c_str(),
                  ec,
                  response.ShortDebugString().c_str());
    } else {
        AUTIL_LOG(INFO, "change topic[%s] part count to[%d] success", topicName.c_str(), newPartCount);
    }
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    return ec;
}

ErrorCode SwiftAdminAdapter::updateWriterVersion(const protocol::TopicWriterVersionInfo &writerVersionInfo) {
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_adminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }
    ec = doUpdateWriterVersion(writerVersionInfo);
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    return ec;
}

ErrorCode SwiftAdminAdapter::doUpdateWriterVersion(const protocol::TopicWriterVersionInfo &writerVersionInfo) {
    ErrorCode ec = ERROR_NONE;
    UpdateWriterVersionRequest request;
    UpdateWriterVersionResponse response;
    *(request.mutable_topicwriterversion()) = writerVersionInfo;
    addRequestExtendInfo(request);
    DO_ADMIN_COMMON_RPC(_adminClient, updateWriterVersion, response);
    return ec;
}

void SwiftAdminAdapter::setLatelyErrorTimeMinIntervalUs(int64_t interval) { _latelyErrorTimeMinInterval = interval; }
void SwiftAdminAdapter::setLatelyErrorTimeMaxIntervalUs(int64_t interval) { _latelyErrorTimeMaxInterval = interval; }
void SwiftAdminAdapter::setAuthenticationConf(const auth::ClientAuthorizerInfo &conf) { _authConf = conf; }
auth::ClientAuthorizerInfo SwiftAdminAdapter::getAuthenticationConf() { return _authConf; }

protocol::ErrorCode SwiftAdminAdapter::modifyTopic(protocol::TopicCreationRequest &request,
                                                   protocol::TopicCreationResponse &response) {
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_adminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }
    addRequestExtendInfo(request);
    DO_ADMIN_COMMON_RPC(_adminClient, modifyTopic, response);
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    return ec;
}

protocol::ErrorCode SwiftAdminAdapter::getMasterInfo(protocol::EmptyRequest &request,
                                                     protocol::MasterInfoResponse &response) {
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_adminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }
    addRequestExtendInfo(request);
    DO_ADMIN_COMMON_RPC(_adminClient, getMasterInfo, response);
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    return ec;
}

protocol::ErrorCode SwiftAdminAdapter::topicAclManage(protocol::TopicAclRequest &request,
                                                      protocol::TopicAclResponse &response) {
    ScopedLock lock(_mutex);
    CHECK_LATELY_ERROR;
    if (!_adminClient) {
        ec = createAdminClient();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN, "create admin client error!");
            checkError(ec, now);
            return ec;
        }
    }
    addRequestExtendInfo(request);
    DO_ADMIN_COMMON_RPC(_adminClient, topicAclManage, response);
    if (needResetAdminClient(ec)) {
        resetAdminClient();
    }
    checkError(ec, now);
    return ec;
}

} // namespace network
} // namespace swift
