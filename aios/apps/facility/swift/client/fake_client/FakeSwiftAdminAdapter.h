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

#include <cstdint>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "swift/client/fake_client/FakeSwiftAdminClient.h"
#include "swift/common/Common.h"
#include "swift/common/FieldSchema.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"

using namespace autil;

namespace swift {
namespace client {

class FakeSwiftAdminAdapter : public network::SwiftAdminAdapter {
public:
    typedef std::pair<uint32_t, std::string> SchemaCacheItem; // timestamp in sec, schema string
public:
    FakeSwiftAdminAdapter();
    ~FakeSwiftAdminAdapter();

private:
    FakeSwiftAdminAdapter(const FakeSwiftAdminAdapter &);
    FakeSwiftAdminAdapter &operator=(const FakeSwiftAdminAdapter &);

public:
    virtual protocol::ErrorCode getBrokerAddress(const std::string &topicName,
                                                 uint32_t partitionId,
                                                 std::string &brokerAddress,
                                                 protocol::AuthenticationInfo authInfo) {
        if (_errorCode != protocol::ERROR_NONE) {
            return _errorCode;
        }
        brokerAddress = topicName + "_" + StringUtil::toString(partitionId);
        return protocol::ERROR_NONE;
    }

    virtual protocol::ErrorCode getBrokerInfo(const std::string &topicName,
                                              uint32_t partitionId,
                                              std::string &brokerAddress,
                                              protocol::BrokerVersionInfo &versionInfo,
                                              protocol::AuthenticationInfo authInfo) {
        if (_errorCode != protocol::ERROR_NONE) {
            return _errorCode;
        }
        brokerAddress = topicName + "_" + StringUtil::toString(partitionId);
        versionInfo = protocol::BrokerVersionInfo();
        return _errorCode;
    }

    virtual protocol::ErrorCode getPartitionCount(const std::string &topicName, uint32_t &partitionCount) {
        (void)topicName;
        if (_errorCode != protocol::ERROR_NONE) {
            return _errorCode;
        }
        partitionCount = _partCount;
        return _errorCode;
    }

    virtual protocol::ErrorCode getPartitionCount(const std::string &topicName,
                                                  int64_t topicVersion,
                                                  uint32_t &partitionCount,
                                                  uint32_t &rangeCountInPartition) {
        (void)topicName;
        (void)topicVersion;
        if (_errorCode != protocol::ERROR_NONE) {
            return _errorCode;
        }
        partitionCount = _partCount;
        rangeCountInPartition = _rangeCountInPartition;
        return _errorCode;
    }

    virtual protocol::ErrorCode getTopicInfo(const std::string &topicName,
                                             protocol::TopicInfoResponse &response,
                                             int64_t topicVersion,
                                             protocol::AuthenticationInfo authInfo) {
        _callCnt++;
        std::ostringstream oss;
        for (auto ecPair : _errorCodeMap) {
            oss << ecPair.first << ":" << ErrorCode_Name(ecPair.second) << ",";
        }
        AUTIL_LOG(INFO,
                  "[callcnt=%d]fake get topic info[%s] ec[%s] ecMap[%s]",
                  _callCnt,
                  topicName.c_str(),
                  ErrorCode_Name(_errorCode).c_str(),
                  oss.str().c_str());
        if (_errorCode != protocol::ERROR_NONE) {
            return _errorCode;
        }
        protocol::ErrorInfo *ei = response.mutable_errorinfo();
        ei->set_errcode(protocol::ERROR_NONE);

        protocol::TopicInfo *ti = response.mutable_topicinfo();
        ti->set_name(topicName);
        ti->set_needfieldfilter(false);
        ti->set_partitioncount(_partCount);
        ti->set_needschema(_needSchema);
        ti->set_topictype(_topicType);
        ti->set_sealed(_sealed);
        if (_hasPermission) {
            *(ti->mutable_opcontrol()) = _opControl;
        }
        for (const auto &name : _physicTopicLst) {
            ti->add_physictopiclst(name);
        }
        if (!_errorCodeMap.empty() && _errorCodeMap.find(_callCnt) != _errorCodeMap.end()) {
            return _errorCodeMap[_callCnt];
        }
        return _errorCode;
    }

    virtual protocol::ErrorCode registerSchema(protocol::RegisterSchemaRequest &request,
                                               protocol::RegisterSchemaResponse &response) {
        if (_errorCode != protocol::ERROR_NONE) {
            return _errorCode;
        }
        common::FieldSchema schema;
        schema.fromJsonString(request.schema());
        int32_t version = request.has_version() ? request.version() : schema.calcVersion();
        uint32_t curTime = autil::TimeUtility::currentTimeInSeconds();
        SchemaCacheItem item(curTime, schema.toJsonString());
        _schemaCache.insert(std::make_pair(version, item));
        protocol::ErrorInfo *ei = response.mutable_errorinfo();
        ei->set_errcode(protocol::ERROR_NONE);
        response.set_version(version);
        return _errorCode;
    }

    virtual protocol::ErrorCode getSchema(protocol::GetSchemaRequest &request,
                                          protocol::GetSchemaResponse &response,
                                          protocol::AuthenticationInfo authInfo) {
        if (_errorCode != protocol::ERROR_NONE) {
            return _errorCode;
        }
        if (0 == request.version()) {
            uint32_t latestTime = 0;
            std::map<int32_t, SchemaCacheItem>::iterator retIter = _schemaCache.end();
            for (auto iter = _schemaCache.begin(); iter != _schemaCache.end(); ++iter) {
                if (iter->second.first > latestTime // topicName is right
                    && iter->second.second.find(request.topicname()) != std::string::npos) {
                    latestTime = iter->second.first;
                    retIter = iter;
                }
            }
            if (_schemaCache.end() != retIter) {
                protocol::SchemaInfo *si = response.mutable_schemainfo();
                si->set_version(retIter->first);
                si->set_registertime(retIter->second.first);
                si->set_schema(retIter->second.second);
                return protocol::ERROR_NONE;
            } else {
                return protocol::ERROR_ADMIN_SCHEMA_NOT_FOUND;
            }
        }
        auto iter = _schemaCache.find(request.version());
        if (_schemaCache.end() != iter // topicName is right
            && iter->second.second.find(request.topicname()) != std::string::npos) {
            protocol::SchemaInfo *si = response.mutable_schemainfo();
            si->set_version(request.version());
            si->set_registertime(iter->second.first);
            si->set_schema(iter->second.second);
            if (0 != _schemaRetVersion) {
                si->set_version(_schemaRetVersion);
            }
            return protocol::ERROR_NONE;
        } else {
            return protocol::ERROR_ADMIN_SCHEMA_NOT_FOUND;
        }
    }

public:
    void setErrorCode(protocol::ErrorCode errorCode) { _errorCode = errorCode; }
    void setPartCount(uint32_t partCount) { _partCount = partCount; }
    void setRangeCountInPartition(uint32_t rangeCountInPartition) { _rangeCountInPartition = rangeCountInPartition; }
    void setNeedSchema(bool needSchema) { _needSchema = needSchema; }
    void setSchemaRetVersion(uint32_t version) { _schemaRetVersion = version; }
    void setTopicType(protocol::TopicType topicType) { _topicType = topicType; }
    void setPhysicTopicLst(const std::vector<std::string> &physicTopicLst) { _physicTopicLst = physicTopicLst; }
    void setSealed(bool sealed) { _sealed = sealed; }
    void setCallCnt(int32_t callCnt) { _callCnt = callCnt; }
    void setErrorCodeMap(const std::map<int32_t, protocol::ErrorCode> &errorCodeMap) { _errorCodeMap = errorCodeMap; }
    void setTopicPermission(protocol::TopicPermission opControl) { _opControl = opControl; }
    void setHasPermission(bool has) { _hasPermission = has; }

public:
    virtual protocol::ErrorCode createAdminClient() {
        if (_errorCode != protocol::ERROR_NONE) {
            return _errorCode;
        }
        _adminClient.reset(new FakeSwiftAdminClient());
        _readAdminClient = _adminClient;
        return _errorCode;
    }

private:
    uint32_t _partCount;
    uint32_t _rangeCountInPartition;
    protocol::ErrorCode _errorCode;
    bool _needSchema;
    int32_t _schemaRetVersion;
    std::map<int32_t, SchemaCacheItem> _schemaCache;
    protocol::TopicType _topicType;
    std::vector<std::string> _physicTopicLst;
    bool _sealed;
    int32_t _callCnt;
    std::map<int32_t, protocol::ErrorCode> _errorCodeMap;
    protocol::TopicPermission _opControl;
    bool _hasPermission = false;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FakeSwiftAdminAdapter);

} // namespace client
} // namespace swift
