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
#include "swift/client/SwiftSchemaReaderImpl.h"

#include <cstddef>
#include <memory>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "swift/client/SwiftReaderConfig.h"
#include "swift/common/SchemaReader.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/MessageUtil.h"

namespace swift {
namespace monitor {
class ClientMetricsReporter;
} // namespace monitor
} // namespace swift

using namespace std;
using namespace autil;
using namespace swift::protocol;
using namespace swift::common;
using namespace swift::network;
namespace swift {
namespace client {

AUTIL_LOG_SETUP(swift, SwiftSchemaReaderImpl);

SwiftSchemaReaderImpl::SwiftSchemaReaderImpl(const SwiftAdminAdapterPtr &adminAdapter,
                                             const SwiftRpcChannelManagerPtr &channelManager,
                                             const TopicInfo &topicInfo,
                                             Notifier *notifier,
                                             string logicTopicName,
                                             monitor::ClientMetricsReporter *reporter)
    : SwiftReaderImpl(adminAdapter, channelManager, topicInfo, notifier, logicTopicName, reporter) {}

SwiftSchemaReaderImpl::~SwiftSchemaReaderImpl() {
    for (auto &iter : _schemaReaderCache) {
        DELETE_AND_SET_NULL(iter.second);
    }
}

ErrorCode SwiftSchemaReaderImpl::init(const SwiftReaderConfig &config) {
    ErrorCode ec = SwiftReaderImpl::init(config);
    if (ERROR_NONE != ec) {
        return ec;
    }
    int32_t retVersion = 0;
    string schemaStr;
    ec = getSchema(0, retVersion, schemaStr);
    if (ERROR_NONE != ec) {
        return ec;
    }
    SchemaReader *reader = new SchemaReader();
    if (reader->loadSchema(schemaStr)) {
        AUTIL_LOG(INFO, "schema cache add schema[%s], version[%d]", schemaStr.c_str(), retVersion);
        _schemaReaderCache.insert(make_pair(retVersion, reader));
    } else {
        AUTIL_LOG(ERROR, "schema[%s] load error, version[%d]", schemaStr.c_str(), retVersion);
        DELETE_AND_SET_NULL(reader);
        return ERROR_CLIENT_LOAD_SCHEMA_FAIL;
    }
    return ec;
}

ErrorCode SwiftSchemaReaderImpl::read(int64_t &timeStamp, Message &msg, int64_t timeout) {
    ErrorCode ec = SwiftReaderImpl::read(timeStamp, msg, timeout);
    if (ERROR_NONE != ec) {
        return ec;
    }

    if (!util::MessageUtil::readDataHead(msg)) {
        AUTIL_LOG(ERROR, "schema msg[%s] invalid", msg.ShortDebugString().c_str());
        return ERROR_CLIENT_SCHEMA_MSG_INVALID;
    }
    return ec;
}

ErrorCode SwiftSchemaReaderImpl::read(int64_t &timeStamp, protocol::Messages &msgs, int64_t timeout) {
    ErrorCode ec = SwiftReaderImpl::read(timeStamp, msgs, timeout);
    if (ERROR_NONE != ec) {
        return ec;
    }
    for (int idx = 0; idx < msgs.msgs_size(); ++idx) {
        Message *one = msgs.mutable_msgs(idx);
        if (!util::MessageUtil::readDataHead(*one)) {
            AUTIL_LOG(ERROR, "schema msg[%s] invalid", msgs.msgs(idx).ShortDebugString().c_str());
            ec = ERROR_CLIENT_SCHEMA_MSG_INVALID;
        }
    }
    return ec;
}

SchemaReader *SwiftSchemaReaderImpl::getSchemaReader(const char *data, ErrorCode &ec) {
    int32_t version = 0;
    if (SchemaReader::readVersion(data, version)) {
        auto iter = _schemaReaderCache.find(version);
        if (_schemaReaderCache.end() != iter) {
            ec = ERROR_NONE;
            return iter->second;
        } else {
            int32_t retVersion = 0;
            string schemaStr;
            ec = getSchema(version, retVersion, schemaStr);
            if (ERROR_NONE != ec) {
                AUTIL_LOG(INFO, "get schema fail, error[%d]", ec);
                return NULL;
            }
            SchemaReader *reader = new SchemaReader();
            if (reader->loadSchema(schemaStr)) {
                AUTIL_LOG(INFO, "schema cache add schema[%s]", schemaStr.c_str());
                _schemaReaderCache.insert(make_pair(retVersion, reader));
                ec = ERROR_NONE;
                return reader;
            } else {
                ec = ERROR_CLIENT_LOAD_SCHEMA_FAIL;
                AUTIL_LOG(WARN, "schema[%s] load error", schemaStr.c_str());
                DELETE_AND_SET_NULL(reader);
                return NULL;
            }
        }
    }
    ec = ERROR_CLIENT_SCHEMA_READ_VERSION;
    return NULL;
}

ErrorCode SwiftSchemaReaderImpl::getSchema(int32_t version, int32_t &retVersion, string &schema) {
    ErrorCode ec = ERROR_NONE;
    auto iter = _schemaCache.find(version);
    if (_schemaCache.end() != iter) {
        retVersion = version;
        schema = iter->second;
    } else {
        GetSchemaResponse response;
        GetSchemaRequest request;
        request.set_topicname(_logicTopicName);
        request.set_version(version);
        protocol::AuthenticationInfo authInfo;
        *(authInfo.mutable_accessauthinfo()) = _config.getAccessAuthInfo();
        ec = _adminAdapter->getSchema(request, response, authInfo);
        if (ERROR_NONE != ec) {
            AUTIL_LOG(ERROR, "get schema fail for topic[%s], version[%d]", _logicTopicName.c_str(), version);
        } else {
            if (response.schemainfo().schema().empty()) {
                AUTIL_LOG(ERROR, "schema is empty, topic[%s], version[%d]", _logicTopicName.c_str(), version);
                ec = ERROR_UNKNOWN;
            } else {
                retVersion = response.schemainfo().version();
                _schemaCache.insert(make_pair(retVersion, response.schemainfo().schema()));
                schema = response.schemainfo().schema();
            }
        }
    }
    return ec;
}

} // namespace client
} // namespace swift
