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
#include "build_service/util/SwiftTopicStreamReaderCreator.h"

#include <algorithm>
#include <ext/alloc_traits.h>
#include <stddef.h>
#include <utility>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "build_service/util/Log.h"
#include "swift/client/SwiftClient.h"
#include "swift/client/SwiftClientConfig.h"
#include "swift/client/SwiftReader.h"
#include "swift/client/SwiftReaderConfig.h"
#include "swift/client/SwiftTopicStreamReader.h"
#include "swift/protocol/ErrCode.pb.h"

namespace build_service::util {
BS_DECLARE_AND_SETUP_LOGGER(SwiftTopicStreamRawDocumentReader);

const std::string SwiftTopicStreamReaderCreator::TOPIC_LIST = "topic_list";
const std::string SwiftTopicStreamReaderCreator::OUTTER_SEP = "#";
const std::string SwiftTopicStreamReaderCreator::INNER_SEP = ":";

std::unique_ptr<swift::client::SwiftReader> SwiftTopicStreamReaderCreator::create(swift::client::SwiftClient* client,
                                                                                  const std::string& topicListStr,
                                                                                  const std::string& readerConfigStr,
                                                                                  const std::string& zkRoot)
{
    std::vector<TopicDesc> topicVec;
    if (!parseTopicList(topicListStr, topicVec) || topicVec.empty()) {
        BS_LOG(ERROR, "invalid topic_list %s", topicListStr.c_str());
        return nullptr;
    }

    auto zkList = autil::StringUtil::split(zkRoot, swift::client::SwiftClientConfig::CLIENT_CONFIG_MULTI_SEPERATOR);
    if (zkList.size() != 1 && zkList.size() != topicVec.size()) {
        BS_LOG(ERROR, "zk count: %lu, topic count: %lu", zkList.size(), topicVec.size());
        return nullptr;
    }

    std::vector<swift::client::SingleTopicReader> topicReaderVec;
    for (size_t i = 0; i < topicVec.size(); ++i) {
        const auto& topicDesc = topicVec[i];
        const auto& zkRoot = zkList.size() == 1 ? "" : zkList[i];
        auto reader = createReaderForTopic(client, zkRoot, topicDesc.topicName, topicDesc.startTimestamp,
                                           topicDesc.stopTimestamp, readerConfigStr);
        if (!reader) {
            return nullptr;
        }
        swift::client::SingleTopicReader topicReader {std::move(reader), std::move(topicDesc.topicName),
                                                      topicDesc.startTimestamp, topicDesc.stopTimestamp};
        topicReaderVec.emplace_back(std::move(topicReader));
    }

    auto reader = std::make_unique<swift::client::SwiftTopicStreamReader>();
    auto ec = reader->init(std::move(topicReaderVec));
    if (ec != swift::protocol::ERROR_NONE) {
        BS_LOG(ERROR, "init SwiftTopicStreamReader failed, error: %s", swift::protocol::ErrorCode_Name(ec).c_str());
        return nullptr;
    }
    return reader;
}

bool SwiftTopicStreamReaderCreator::parseTopicList(const std::string& str, std::vector<TopicDesc>& topicVec)
{
    auto topicStrVec = autil::StringUtil::split(str, OUTTER_SEP);
    for (const auto& topicStr : topicStrVec) {
        auto values = autil::StringUtil::split(topicStr, INNER_SEP);
        if (values.size() != 3) {
            BS_LOG(ERROR, "invalid topic format: %s", topicStr.c_str());
            return false;
        }
        TopicDesc td;
        td.topicName = std::move(values[0]);
        if (!autil::StringUtil::fromString(values[1], td.startTimestamp)) {
            BS_LOG(ERROR, "startTimestamp %s is invalid", values[1].c_str());
            return false;
        }
        td.startTimestamp = std::max((int64_t)0, td.startTimestamp);
        if (!autil::StringUtil::fromString(values[2], td.stopTimestamp)) {
            BS_LOG(ERROR, "stopTimestamp %s is invalid", values[2].c_str());
            return false;
        }
        td.stopTimestamp = td.stopTimestamp < 0 ? std::numeric_limits<int64_t>::max() : td.stopTimestamp;
        if (td.stopTimestamp <= td.startTimestamp) {
            BS_LOG(ERROR, "startTimestamp[%ld] <= stopTimestamp[%ld]", td.startTimestamp, td.stopTimestamp);
            return false;
        }
        topicVec.emplace_back(std::move(td));
    }
    return true;
}

std::unique_ptr<swift::client::SwiftReader>
SwiftTopicStreamReaderCreator::createReaderForTopic(swift::client::SwiftClient* client, const std::string& zkRoot,
                                                    const std::string& topicName, int64_t startTimestamp,
                                                    int64_t stopTimestamp, const std::string& readerConfig)
{
    std::unique_ptr<swift::client::SwiftReader> reader;
    std::string readerConfigStr;
    if (!zkRoot.empty()) {
        readerConfigStr += std::string(swift::client::READER_CONFIG_ZK_PATH) + "=" + zkRoot + ";";
    }
    readerConfigStr += std::string(swift::client::READER_CONFIG_TOPIC_NAME) + "=" + topicName;
    if (!readerConfig.empty()) {
        readerConfigStr += ";" + readerConfig;
    }
    swift::protocol::ErrorInfo errorInfo;
    reader.reset(client->createReader(readerConfigStr, &errorInfo));
    if (!reader) {
        BS_LOG(ERROR, "create swift reader with %s failed, error: %s", readerConfigStr.c_str(),
               errorInfo.ShortDebugString().c_str());
        return nullptr;
    }
    if (startTimestamp > 0) {
        auto ec = reader->seekByTimestamp(startTimestamp, true);
        if (ec != swift::protocol::ERROR_NONE) {
            BS_LOG(ERROR, "seek to %ld for topic %s failed, error: %s", startTimestamp, topicName.c_str(),
                   swift::protocol::ErrorCode_Name(ec).c_str());
            return nullptr;
        }
    }
    if (stopTimestamp != std::numeric_limits<int64_t>::max()) {
        reader->setTimestampLimit(stopTimestamp, stopTimestamp);
    }
    return reader;
}

} // namespace build_service::util
