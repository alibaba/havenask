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
#include "swift/client/fake_client/FakeSwiftAdminClient.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <google/protobuf/stubs/port.h>
#include <utility>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"

using namespace std;
using namespace arpc;
using namespace swift;
using namespace autil;
using namespace google::protobuf;

using namespace swift::protocol;
using namespace swift::network;
namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, FakeSwiftAdminClient);

FakeSwiftAdminClient::FakeSwiftAdminClient()
    : SwiftAdminClient("", SwiftRpcChannelManagerPtr())
    , _timeoutCount(0)
    , _errorCode(ERROR_NONE)
    , _topicModifyTime(-1)
    , _getTopicInfoCount(0)
    , _rpcFail(false)
    , _needPartInfoAddress(false) {}

FakeSwiftAdminClient::~FakeSwiftAdminClient() {
    for (uint32_t i = 0; i < _partInfo.size(); ++i) {
        delete _partInfo[i];
    }
    _partInfo.clear();
}

bool FakeSwiftAdminClient::createTopic(const TopicCreationRequest *request,
                                       TopicCreationResponse *response,
                                       int64_t timeout) {
    (void)timeout;
    if (_errorCode != ERROR_NONE) {
        swift::protocol::ErrorInfo *error = response->mutable_errorinfo();
        error->set_errcode(_errorCode);
        error->set_errmsg("admin error");
        return true;
    }
    if (!checkAuthority(request, response)) {
        return true;
    }
    auto iter = _topicInfo.find(request->topicname());
    if (iter != _topicInfo.end()) {
        swift::protocol::ErrorInfo *error = response->mutable_errorinfo();
        error->set_errcode(ERROR_ADMIN_TOPIC_HAS_EXISTED);
    } else {
        _topicInfo[request->topicname()] = *request;
        swift::protocol::ErrorInfo *error = response->mutable_errorinfo();
        error->set_errcode(ERROR_NONE);
    }
    return true;
}

void FakeSwiftAdminClient::createTopic(const string &topicName, uint32_t partCount) {
    TopicCreationRequest meta;
    meta.set_topicname(topicName);
    meta.set_partitioncount(partCount);
    _topicInfo[topicName] = meta;
}

bool FakeSwiftAdminClient::modifyTopic(const TopicCreationRequest *request,
                                       TopicCreationResponse *response,
                                       int64_t timeout) {
    (void)request;
    (void)response;
    (void)timeout;
    if (_rpcFail) {
        return false;
    }
    if (!checkAuthority(request, response)) {
        return true;
    }
    auto iter = _topicInfo.find(request->topicname());
    if (_topicInfo.end() == iter) {
        response->mutable_errorinfo()->set_errcode(ERROR_ADMIN_TOPIC_NOT_EXISTED);
    } else {
        if (request->has_partitioncount()) {
            iter->second.set_partitioncount(request->partitioncount());
        }
        iter->second.set_sealed(request->sealed());
        response->mutable_errorinfo()->set_errcode(ERROR_NONE);
    }
    return true;
}

bool FakeSwiftAdminClient::deleteTopic(const TopicDeletionRequest *request,
                                       TopicDeletionResponse *response,
                                       int64_t timeout) {
    (void)timeout;
    if (_errorCode != ERROR_NONE) {
        swift::protocol::ErrorInfo *error = response->mutable_errorinfo();
        error->set_errcode(_errorCode);
        error->set_errmsg("admin error");
        return true;
    }
    if (!checkAuthority(request, response)) {
        return true;
    }
    auto iter = _topicInfo.find(request->topicname());
    if (iter != _topicInfo.end()) {
        _topicInfo.erase(iter);
        swift::protocol::ErrorInfo *error = response->mutable_errorinfo();
        error->set_errcode(ERROR_NONE);
    } else {
        swift::protocol::ErrorInfo *error = response->mutable_errorinfo();
        error->set_errcode(ERROR_ADMIN_TOPIC_NOT_EXISTED);
    }
    return true;
}

bool FakeSwiftAdminClient::createTopicBatch(const TopicBatchCreationRequest *request,
                                            TopicBatchCreationResponse *response,
                                            int64_t timeout) {
    (void)timeout;
    if (_errorCode != ERROR_NONE) {
        swift::protocol::ErrorInfo *error = response->mutable_errorinfo();
        error->set_errcode(_errorCode);
        error->set_errmsg("admin error");
        return true;
    }

    if (!checkAuthority(request, response)) {
        return true;
    }
    vector<string> existTopics;
    for (int idx = 0; idx < request->topicrequests_size(); ++idx) {
        const TopicCreationRequest &meta = request->topicrequests(idx);
        auto iter = _topicInfo.find(meta.topicname());
        if (iter != _topicInfo.end()) {
            existTopics.emplace_back(meta.topicname());
        }
    }
    if (existTopics.size() > 0) {
        swift::protocol::ErrorInfo *error = response->mutable_errorinfo();
        error->set_errcode(ERROR_ADMIN_TOPIC_HAS_EXISTED);
        const string &msg = StringUtil::toString(existTopics, ";");
        error->set_errmsg("ERROR_ADMIN_TOPIC_HAS_EXISTED[" + msg + "]");
        return true;
    }

    for (int idx = 0; idx < request->topicrequests_size(); ++idx) {
        const TopicCreationRequest &meta = request->topicrequests(idx);
        _topicInfo[meta.topicname()] = meta;
    }
    swift::protocol::ErrorInfo *error = response->mutable_errorinfo();
    error->set_errcode(ERROR_NONE);
    return true;
}

bool FakeSwiftAdminClient::deleteTopicBatch(const TopicBatchDeletionRequest *request,
                                            TopicBatchDeletionResponse *response,
                                            int64_t timeout) {
    (void)timeout;
    if (_errorCode != ERROR_NONE) {
        swift::protocol::ErrorInfo *error = response->mutable_errorinfo();
        error->set_errcode(_errorCode);
        error->set_errmsg("admin error");
        return true;
    }

    if (!checkAuthority(request, response)) {
        return true;
    }
    bool deleted = false;
    for (int i = 0; i < request->topicnames_size(); i++) {
        auto iter = _topicInfo.find(request->topicnames(i));
        if (iter != _topicInfo.end()) {
            _topicInfo.erase(iter);
            swift::protocol::ErrorInfo *error = response->mutable_errorinfo();
            error->set_errcode(ERROR_NONE);
            deleted = true;
        }
    }

    swift::protocol::ErrorInfo *error = response->mutable_errorinfo();
    if (deleted) {
        error->set_errcode(ERROR_NONE);
    } else {
        error->set_errcode(ERROR_ADMIN_TOPIC_NOT_EXISTED);
    }
    return true;
}

bool FakeSwiftAdminClient::updateWriterVersion(const protocol::UpdateWriterVersionRequest *request,
                                               protocol::UpdateWriterVersionResponse *response,
                                               int64_t timeout) {
    (void)timeout;
    if (_errorCode != ERROR_NONE) {
        swift::protocol::ErrorInfo *error = response->mutable_errorinfo();
        error->set_errcode(_errorCode);
        error->set_errmsg("admin error");
        return true;
    }
    if (_rpcFail) {
        return false;
    }
    if (!checkAuthority(request, response)) {
        return true;
    }
    _topicWriterVersion[request->topicwriterversion().topicname()] = request->topicwriterversion().majorversion();
    return true;
}

bool FakeSwiftAdminClient::getSysInfo(const EmptyRequest *request, SysInfoResponse *response, int64_t timeout) {
    (void)request;
    (void)response;
    (void)timeout;
    assert(false);
    return false;
}

bool FakeSwiftAdminClient::getTopicInfo(const TopicInfoRequest *request, TopicInfoResponse *response, int64_t timeout) {
    (void)timeout;
    _getTopicInfoCount++;
    if (_timeoutCount > 0) {
        --_timeoutCount;
        return false;
    }

    if (_errorCode != ERROR_NONE) {
        swift::protocol::ErrorInfo *error = response->mutable_errorinfo();
        error->set_errcode(_errorCode);
        error->set_errmsg("admin error");
        return true;
    }
    if (!checkAuthority(request, response)) {
        return true;
    }
    string topicName = request->topicname();
    auto it = _topicInfo.find(topicName);
    if (it == _topicInfo.end()) {
        // TopicInfo *topicInfo = response->mutable_topicinfo();
        // topicInfo = NULL; // avoid warn
        swift::protocol::ErrorInfo *error = response->mutable_errorinfo();
        error->set_errcode(ERROR_ADMIN_TOPIC_NOT_EXISTED);
        error->set_errmsg("unknown topic name");
    } else {
        TopicInfo *topicInfo = response->mutable_topicinfo();
        topicInfo->set_name(request->topicname());
        topicInfo->set_status(TOPIC_STATUS_RUNNING);
        topicInfo->set_partitioncount(it->second.partitioncount());
        topicInfo->set_topictype(it->second.topictype());
        for (size_t i = 0; i < it->second.partitioncount(); ++i) {
            PartitionInfo *partitionInfo = topicInfo->add_partitioninfos();
            partitionInfo->set_id(i);
            if (_needPartInfoAddress) {
                partitionInfo->set_brokeraddress("10.10.10." + to_string(i) + ":0000");
            }
            BrokerVersionInfo versionInfo;
            versionInfo.set_version("1");
            *(partitionInfo->mutable_versioninfo()) = versionInfo;
        }
        if (_topicModifyTime > 0) {
            topicInfo->set_modifytime(_topicModifyTime);
        }
        swift::protocol::ErrorInfo *error = response->mutable_errorinfo();
        error->set_errcode(ERROR_NONE);
        error->set_errmsg("");
    }
    return true;
}

bool FakeSwiftAdminClient::getAllTopicName(const EmptyRequest *request, TopicNameResponse *response, int64_t timeout) {
    (void)request;
    (void)response;
    (void)timeout;
    assert(false);
    return false;
}

bool FakeSwiftAdminClient::getPartitionInfo(const PartitionInfoRequest *request,
                                            PartitionInfoResponse *response,
                                            int64_t timeout) {
    (void)response;
    (void)timeout;
    if (_timeoutCount > 0) {
        --_timeoutCount;
        return false;
    }

    if (_errorCode != ERROR_NONE) {
        swift::protocol::ErrorInfo *error = response->mutable_errorinfo();
        error->set_errcode(_errorCode);
        error->set_errmsg("admin error");
        return true;
    }
    if (!checkAuthority(request, response)) {
        return true;
    }
    swift::protocol::ErrorInfo *error = response->mutable_errorinfo();
    error->set_errcode(ERROR_NONE);
    error->set_errmsg("");

    uint32_t partitionCount = request->partitionids().size();
    for (uint32_t i = 0; i < partitionCount; ++i) {
        uint32_t partitionId = request->partitionids(i);
        map<uint32_t, PartitionInfoResponse *>::iterator it = _partInfo.find(partitionId);
        if (it == _partInfo.end()) {
            swift::protocol::PartitionInfo *partInfo = response->add_partitioninfos();
            partInfo->set_id(partitionId);
            partInfo->set_brokeraddress("127.0.0.1:7777");
            partInfo->set_status(PARTITION_STATUS_RUNNING);
        } else {
            response->CopyFrom(*(it->second));
        }
    }
    return true;
}

bool FakeSwiftAdminClient::getAllMachineAddress(const EmptyRequest *request,
                                                MachineAddressResponse *response,
                                                int64_t timeout) {
    (void)request;
    (void)response;
    (void)timeout;
    assert(false);
    return false;
}

bool FakeSwiftAdminClient::getRoleAddress(const RoleAddressRequest *request,
                                          RoleAddressResponse *response,
                                          int64_t timeout) {
    (void)request;
    (void)response;
    (void)timeout;
    assert(false);
    return false;
}

} // namespace client
} // namespace swift
