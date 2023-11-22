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

#include <limits>
#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

namespace swift::client {
class SwiftClient;
class SwiftReader;
} // namespace swift::client

namespace build_service::util {

class SwiftTopicStreamReaderCreator
{
public:
    static std::unique_ptr<swift::client::SwiftReader> create(swift::client::SwiftClient* client,
                                                              const std::string& topicListStr,
                                                              const std::string& readerConfigStr,
                                                              const std::string& zkRoot = "");

private:
    struct TopicDesc {
        std::string topicName;
        int64_t startTimestamp = 0;
        int64_t stopTimestamp = std::numeric_limits<int64_t>::max();
    };
    static bool parseTopicList(const std::string& str, std::vector<TopicDesc>& topicVec);
    static std::unique_ptr<swift::client::SwiftReader>
    createReaderForTopic(swift::client::SwiftClient* client, const std::string& zkRoot, const std::string& topicName,
                         int64_t startTimestamp, int64_t stopTimestamp, const std::string& readerConfig);

public:
    static const std::string TOPIC_LIST;
    static const std::string OUTTER_SEP;
    static const std::string INNER_SEP;
};

} // namespace build_service::util
