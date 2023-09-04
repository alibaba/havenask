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
#include <stddef.h>
#include <string>
#include <unordered_map>

#include "autil/Log.h"
#include "swift/client/SwiftReaderImpl.h"
#include "swift/common/Common.h"
#include "swift/common/SchemaReader.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace monitor {
class ClientMetricsReporter;
} // namespace monitor

namespace protocol {
class Message;
class Messages;

class TopicInfo;
} // namespace protocol

namespace client {
class Notifier;
class SwiftReaderConfig;

class SwiftSchemaReaderImpl : public SwiftReaderImpl {
public:
    SwiftSchemaReaderImpl(const network::SwiftAdminAdapterPtr &adminAdapter,
                          const network::SwiftRpcChannelManagerPtr &channelManager,
                          const protocol::TopicInfo &topicInfo,
                          Notifier *notifier = NULL,
                          std::string logicTopicName = std::string(),
                          monitor::ClientMetricsReporter *reporter = nullptr);
    ~SwiftSchemaReaderImpl();

private:
    SwiftSchemaReaderImpl(const SwiftSchemaReaderImpl &);
    SwiftSchemaReaderImpl &operator=(const SwiftSchemaReaderImpl &);

public:
    protocol::ErrorCode init(const SwiftReaderConfig &config) override;
    protocol::ErrorCode read(int64_t &timeStamp, protocol::Message &msg, int64_t timeout = 3 * 1000000) override;
    protocol::ErrorCode read(int64_t &timeStamp, protocol::Messages &msgs, int64_t timeout = 3 * 1000000) override;
    common::SchemaReader *getSchemaReader(const char *data, protocol::ErrorCode &ec) override;
    protocol::ErrorCode getSchema(int32_t version, int32_t &retVersion, std::string &schema);

private:
    std::unordered_map<int32_t, common::SchemaReader *> _schemaReaderCache;
    std::unordered_map<int32_t, std::string> _schemaCache;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SwiftSchemaReaderImpl);

} // namespace client
} // namespace swift
