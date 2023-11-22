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

namespace swift::client {
class SwiftClient;
class SwiftReader;
} // namespace swift::client

namespace build_service { namespace common {

struct SwiftParam {
    SwiftParam()
        : swiftClient(nullptr)
        , reader(nullptr)
        , topicName("")
        , isMultiTopic(false)
        , disableSwiftMaskFilter(false)
        , maskFilterPairs({})
        , from(0)
        , to(655535)
    {
    }
    SwiftParam(swift::client::SwiftClient* client, swift::client::SwiftReader* reader, const std::string& topicName,
               bool isMultiTopic, const std::vector<std::pair<uint8_t, uint8_t>>& maskFilterPairs, uint32_t from,
               uint32_t to, bool disableSwiftInnerMaskFilter = false)
        : swiftClient(client)
        , reader(reader)
        , topicName(topicName)
        , isMultiTopic(isMultiTopic)
        , disableSwiftMaskFilter(disableSwiftInnerMaskFilter)
        , maskFilterPairs(maskFilterPairs)
        , from(from)
        , to(to)
    {
    }
    swift::client::SwiftClient* swiftClient = nullptr;
    swift::client::SwiftReader* reader = nullptr;
    std::string topicName;
    bool isMultiTopic;
    bool disableSwiftMaskFilter;
    std::vector<std::pair<uint8_t, uint8_t>> maskFilterPairs;
    uint32_t from;
    uint32_t to;
};

}} // namespace build_service::common
