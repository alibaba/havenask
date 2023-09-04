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
#include "swift/client/SwiftSchemaWriterImpl.h"

#include <iosfwd>
#include <memory>
#include <string>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "swift/client/SwiftWriterConfig.h"
#include "swift/common/SchemaWriter.h"
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
namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, SwiftSchemaWriterImpl);

SwiftSchemaWriterImpl::SwiftSchemaWriterImpl(network::SwiftAdminAdapterPtr adminAdapter,
                                             network::SwiftRpcChannelManagerPtr channelManager,
                                             ThreadPoolPtr mergeThreadPool,
                                             const TopicInfo &topicInfo,
                                             BufferSizeLimiterPtr limiter,
                                             string logicTopicName,
                                             monitor::ClientMetricsReporter *reporter)
    : SwiftWriterImpl(adminAdapter, channelManager, mergeThreadPool, topicInfo, limiter, logicTopicName, reporter) {
    _msgWriter = new SchemaWriter();
}

SwiftSchemaWriterImpl::~SwiftSchemaWriterImpl() { DELETE_AND_SET_NULL(_msgWriter); }

ErrorCode SwiftSchemaWriterImpl::init(const SwiftWriterConfig &config, util::BlockPoolPtr blockPool) {
    if (SwiftWriterConfig::DEFAULT_SCHEMA_VERSION != config.schemaVersion) {
        GetSchemaResponse response;
        GetSchemaRequest request;
        request.set_topicname(_logicTopicName);
        request.set_version(config.schemaVersion);
        protocol::AuthenticationInfo authInfo;
        *(authInfo.mutable_accessauthinfo()) = config.getAccessAuthInfo();
        ErrorCode ec = _adminAdapter->getSchema(request, response, authInfo);
        if (ERROR_NONE != ec) {
            AUTIL_LOG(ERROR, "get schema fail[topic:%s, version:%d]", _logicTopicName.c_str(), config.schemaVersion);
            return ec;
        }
        if (!_msgWriter->loadSchema(response.schemainfo().schema())) {
            AUTIL_LOG(ERROR, "load schema fail, schema[%s]", response.schemainfo().schema().c_str());
            return ERROR_CLIENT_LOAD_SCHEMA_FAIL;
        }
        _msgWriter->setVersion(response.schemainfo().version());
        AUTIL_LOG(INFO,
                  "schema writer use version[%d], config version[%d]",
                  response.schemainfo().version(),
                  config.schemaVersion);
    }
    return SwiftWriterImpl::init(config, blockPool);
}

ErrorCode SwiftSchemaWriterImpl::write(MessageInfo &msgInfo) {
    util::MessageUtil::writeDataHead(msgInfo);
    ErrorCode ec = SwiftWriterImpl::write(msgInfo);
    return ec;
}

SchemaWriter *SwiftSchemaWriterImpl::getSchemaWriter() { return _msgWriter; }

} // namespace client
} // namespace swift
