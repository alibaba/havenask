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

#include <string>

#include "autil/Log.h"
#include "autil/ThreadPool.h"
#include "swift/client/BufferSizeLimiter.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/SwiftWriterImpl.h"
#include "swift/common/Common.h"
#include "swift/common/SchemaWriter.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/BlockPool.h"

namespace swift {

namespace monitor {
class ClientMetricsReporter;
} // namespace monitor

namespace protocol {
class TopicInfo;
} // namespace protocol

namespace client {
class SwiftWriterConfig;

class SwiftSchemaWriterImpl : public SwiftWriterImpl {
public:
    SwiftSchemaWriterImpl(network::SwiftAdminAdapterPtr adminAdapter,
                          network::SwiftRpcChannelManagerPtr channelManager,
                          autil::ThreadPoolPtr mergeThreadPool,
                          const protocol::TopicInfo &topicInfo,
                          BufferSizeLimiterPtr limiter = BufferSizeLimiterPtr(),
                          std::string logicTopicName = std::string(),
                          monitor::ClientMetricsReporter *reporter = nullptr);
    ~SwiftSchemaWriterImpl();

private:
    SwiftSchemaWriterImpl(const SwiftSchemaWriterImpl &);
    SwiftSchemaWriterImpl &operator=(const SwiftSchemaWriterImpl &);

public:
    protocol::ErrorCode init(const SwiftWriterConfig &config, util::BlockPoolPtr blockPool) override;
    common::SchemaWriter *getSchemaWriter() override;
    protocol::ErrorCode write(MessageInfo &msgInfo) override;

protected:
    common::SchemaWriter *_msgWriter;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SwiftSchemaWriterImpl);

} // namespace client
} // namespace swift
