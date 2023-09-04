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
#include "suez/drc/SwiftReaderCreator.h"

#include "build_service/util/SwiftClientCreator.h"
#include "build_service/util/SwiftTopicStreamReaderCreator.h"
#include "suez/drc/ParamParser.h"
#include "swift/client/SwiftReader.h"

namespace suez {
AUTIL_LOG_SETUP(suez, SwiftReaderCreator);

bool SwiftSourceConfig::init(const std::map<std::string, std::string> &params) {
    swiftRoot = ParamParser::getParameter(params, "swift_root");
    if (swiftRoot.empty()) {
        return false;
    }
    topicStreamMode = ParamParser::getParameter(params, "topic_stream_mode") == "true";
    topicName = ParamParser::getParameter(params, "topic_name");
    if (!topicStreamMode && topicName.empty()) {
        return false;
    }
    topicListStr = ParamParser::getParameter(params, "topic_list");
    clientConfig = ParamParser::getParameter(params, "swift_client_config");
    readerConfig = ParamParser::getParameter(params, "reader_config");
    if (!ParamParser::getParameterT(params, "need_field_filter", needFieldFilter)) {
        return false;
    }
    if (!ParamParser::getParameterT(params, "from", from)) {
        return false;
    }
    if (!ParamParser::getParameterT(params, "to", to)) {
        return false;
    }
    if (from > to || to > 65535) {
        return false;
    }
    ParamParser::getParameterT(params, "swift_start_timestamp", startTimestamp);
    return true;
}

std::string SwiftSourceConfig::constructReadConfigStr() const {
    std::stringstream ss;
    if (!topicStreamMode) {
        ss << "topicName=" << topicName << ";";
    }
    ss << "from=" << from << ";";
    ss << "to=" << to;
    if (!readerConfig.empty()) {
        ss << ";" << readerConfig;
    }
    return ss.str();
}

std::unique_ptr<swift::client::SwiftReader>
SwiftReaderCreator::create(const std::shared_ptr<build_service::util::SwiftClientCreator> &client,
                           const std::map<std::string, std::string> &param) {
    SwiftSourceConfig swiftSourceConfig;
    if (!swiftSourceConfig.init(param)) {
        AUTIL_LOG(ERROR, "invalid swift source config [%s]", autil::StringUtil::toString(param).c_str());
        return nullptr;
    }
    AUTIL_LOG(INFO,
              "create swift client, swift_root: %s, client config: %s",
              swiftSourceConfig.swiftRoot.c_str(),
              swiftSourceConfig.clientConfig.c_str());
    auto swiftClient = client->createSwiftClient(swiftSourceConfig.swiftRoot, swiftSourceConfig.clientConfig);
    if (!swiftClient) {
        AUTIL_LOG(ERROR,
                  "create swift client failed, swift_root: %s, client config: %s",
                  swiftSourceConfig.swiftRoot.c_str(),
                  swiftSourceConfig.clientConfig.c_str());
        return nullptr;
    }

    std::unique_ptr<swift::client::SwiftReader> reader;
    auto readerConfigStr = swiftSourceConfig.constructReadConfigStr();
    if (!swiftSourceConfig.topicStreamMode) {
        swift::protocol::ErrorInfo error;
        reader.reset(swiftClient->createReader(readerConfigStr, &error));
        if (!reader) {
            AUTIL_LOG(ERROR,
                      "create swift reader with config %s failed, error: %s",
                      readerConfigStr.c_str(),
                      error.ShortDebugString().c_str());
            return nullptr;
        }
        if (swiftSourceConfig.startTimestamp > 0) {
            auto ec = reader->seekByTimestamp(swiftSourceConfig.startTimestamp, true);
            if (ec != swift::protocol::ERROR_NONE) {
                AUTIL_LOG(ERROR,
                          "seek to %ld failed, error: %s",
                          swiftSourceConfig.startTimestamp,
                          swift::protocol::ErrorCode_Name(ec).c_str());
                return nullptr;
            }
        }
    } else {
        reader = build_service::util::SwiftTopicStreamReaderCreator::create(
            swiftClient, swiftSourceConfig.topicListStr, readerConfigStr, swiftSourceConfig.swiftRoot);
        if (!reader) {
            AUTIL_LOG(ERROR,
                      "create swift reader for topic %s with config %s failed",
                      swiftSourceConfig.topicListStr.c_str(),
                      readerConfigStr.c_str());
            return nullptr;
        }
    }
    return reader;
}

} // namespace suez
