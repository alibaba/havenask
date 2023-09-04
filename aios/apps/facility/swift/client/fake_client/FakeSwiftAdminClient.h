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

#include <assert.h>
#include <cstdint>
#include <map>
#include <string>

#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/network/SwiftAdminClient.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/version.h"

namespace swift {
namespace client {

class FakeSwiftAdminClient : public network::SwiftAdminClient {
public:
    FakeSwiftAdminClient();
    virtual ~FakeSwiftAdminClient();

private:
    FakeSwiftAdminClient(const FakeSwiftAdminClient &);
    FakeSwiftAdminClient &operator=(const FakeSwiftAdminClient &);

public:
    using network::SwiftAdminClient::createTopic;
    void createTopic(const std::string &topicName, uint32_t partCount);
    void setTimeoutCount(int32_t timeoutCount) { _timeoutCount = timeoutCount; }

    virtual bool createTopic(const protocol::TopicCreationRequest *request,
                             protocol::TopicCreationResponse *response,
                             int64_t timeout = 20 * 1000);
    virtual bool modifyTopic(const protocol::TopicCreationRequest *request,
                             protocol::TopicCreationResponse *response,
                             int64_t timeout = 20 * 1000);
    virtual bool deleteTopic(const protocol::TopicDeletionRequest *request,
                             protocol::TopicDeletionResponse *response,
                             int64_t timeout = 20 * 1000);
    virtual bool
    getSysInfo(const protocol::EmptyRequest *request, protocol::SysInfoResponse *response, int64_t timeout = 20 * 1000);
    virtual bool getTopicInfo(const protocol::TopicInfoRequest *request,
                              protocol::TopicInfoResponse *response,
                              int64_t timeout = 20 * 1000);
    virtual bool getAllTopicName(const protocol::EmptyRequest *request,
                                 protocol::TopicNameResponse *response,
                                 int64_t timeout = 20 * 1000);
    virtual bool getPartitionInfo(const protocol::PartitionInfoRequest *request,
                                  protocol::PartitionInfoResponse *response,
                                  int64_t timeout = 20 * 1000);
    virtual bool getAllMachineAddress(const protocol::EmptyRequest *request,
                                      protocol::MachineAddressResponse *response,
                                      int64_t timeout = 20 * 1000);
    virtual bool getRoleAddress(const protocol::RoleAddressRequest *request,
                                protocol::RoleAddressResponse *response,
                                int64_t timeout = 20 * 1000);
    virtual bool createTopicBatch(const protocol::TopicBatchCreationRequest *request,
                                  protocol::TopicBatchCreationResponse *response,
                                  int64_t timeout = 20 * 1000);
    virtual bool deleteTopicBatch(const protocol::TopicBatchDeletionRequest *request,
                                  protocol::TopicBatchDeletionResponse *response,
                                  int64_t timeout = 20 * 1000);

    virtual bool updateWriterVersion(const protocol::UpdateWriterVersionRequest *request,
                                     protocol::UpdateWriterVersionResponse *response,
                                     int64_t timeout = 20 * 1000);

public:
    void setPartitionInfoResponse(uint32_t pid, protocol::PartitionInfoResponse *response) {
        if (_partInfo.find(pid) != _partInfo.end()) {
            delete _partInfo[pid];
        }

        _partInfo[pid] = response;
    }
    void setErrorCode(protocol::ErrorCode ec) { _errorCode = ec; }
    void setAccessAuthInfo(const std::string &userName, const std::string &passwd) {
        _userName = userName;
        _passwd = passwd;
    }
    template <typename T1, typename T2>
    bool checkAuthority(const T1 &request, const T2 &response) {
        assert(request->versioninfo().version() == swift_admin_version_str);

        if (_userName.empty() || _passwd.empty()) {
            return true;
        }
        const auto &auth = request->authentication();
        if ((auth.username() == _userName && auth.passwd() == _passwd) ||
            (auth.accessauthinfo().accessid() == _userName && auth.accessauthinfo().accesskey() == _passwd)) {
            return true;
        }

        swift::protocol::ErrorInfo *ei = response->mutable_errorinfo();
        ei->set_errcode(swift::protocol::ERROR_ADMIN_AUTHENTICATION_FAILED);
        ei->set_errmsg(ErrorCode_Name(ei->errcode()));
        return false;
    }

private:
    std::map<std::string, protocol::TopicCreationRequest> _topicInfo;
    std::map<std::string, uint32_t> _topicWriterVersion;
    std::map<uint32_t, protocol::PartitionInfoResponse *> _partInfo;
    int32_t _timeoutCount;
    protocol::ErrorCode _errorCode;
    int64_t _topicModifyTime;
    int32_t _getTopicInfoCount;
    bool _rpcFail;
    bool _needPartInfoAddress;
    std::string _userName;
    std::string _passwd;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FakeSwiftAdminClient);

} // namespace client
} // namespace swift
