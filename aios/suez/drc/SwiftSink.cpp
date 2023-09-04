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
#include "suez/drc/SwiftSink.h"

#include "autil/Log.h"
#include "build_service/util/SwiftClientCreator.h"
#include "suez/drc/DrcConfig.h"
#include "suez/drc/ParamParser.h"
#include "swift/client/SwiftWriter.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez.drc, SwiftSink);

const std::string SwiftSink::TYPE = "swift";

SwiftSink::SwiftSink(std::unique_ptr<swift::client::SwiftWriter> writer) : _writer(std::move(writer)) {}

SwiftSink::~SwiftSink() {}

const std::string &SwiftSink::getType() const { return TYPE; }

bool SwiftSink::write(uint32_t hashId, std::string data, int64_t checkpoint) {
    swift::client::MessageInfo msgInfo;
    msgInfo.data = std::move(data);
    msgInfo.uint16Payload = (uint16_t)hashId;
    msgInfo.checkpointId = checkpoint;

    auto ec = _writer->write(msgInfo);
    if (ec != swift::protocol::ERROR_NONE) {
        if (ec == swift::protocol::ERROR_CLIENT_SEND_BUFFER_FULL) {
            AUTIL_INTERVAL_LOG(300, WARN, "swift send buffer is full");
        } else {
            AUTIL_LOG(ERROR, "write failed, error: %s", swift::protocol::ErrorCode_Name(ec).c_str());
        }
        return false;
    }
    return true;
}

int64_t SwiftSink::getCommittedCheckpoint() const { return _writer->getCommittedCheckpointId(); }

bool SwiftSinkConfig::init(const std::map<std::string, std::string> &params) {
    swiftRoot = ParamParser::getParameter(params, "swift_root");
    if (swiftRoot.empty()) {
        return false;
    }
    topicName = ParamParser::getParameter(params, "topic_name");
    if (topicName.empty()) {
        return false;
    }
    clientConfig = ParamParser::getParameter(params, "swift_client_config");
    writerConfig = ParamParser::getParameter(params, "writer_config");
    return true;
}

std::string SwiftSinkConfig::constructWriterConfigStr() const {
    std::string configStr = "topicName=" + topicName;
    configStr += ";functionChain=hashId2partId"; // simplify configuration
    if (!writerConfig.empty()) {
        configStr += ";" + writerConfig;
    }
    return configStr;
}

std::unique_ptr<Sink> SwiftSink::create(const std::shared_ptr<build_service::util::SwiftClientCreator> &client,
                                        const SinkConfig &config) {
    SwiftSinkConfig swiftSinkConfig;
    if (!swiftSinkConfig.init(config.parameters)) {
        AUTIL_LOG(ERROR, "invalid swift sink config: %s", FastToJsonString(config).c_str());
        return nullptr;
    }
    auto swiftClient = client->createSwiftClient(swiftSinkConfig.swiftRoot, swiftSinkConfig.clientConfig);
    if (!swiftClient) {
        AUTIL_LOG(ERROR,
                  "create swift client failed, swift_root: %s, client config: %s",
                  swiftSinkConfig.swiftRoot.c_str(),
                  swiftSinkConfig.clientConfig.c_str());
        return nullptr;
    }
    auto writerConfigStr = swiftSinkConfig.constructWriterConfigStr();
    swift::protocol::ErrorInfo error;
    std::unique_ptr<swift::client::SwiftWriter> writer(swiftClient->createWriter(writerConfigStr, &error));
    if (!writer) {
        AUTIL_LOG(ERROR,
                  "create swift writer with config %s failed, error: %s",
                  writerConfigStr.c_str(),
                  error.ShortDebugString().c_str());
        return nullptr;
    }
    return std::make_unique<SwiftSink>(std::move(writer));
}

} // namespace suez
