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

#include <map>
#include <memory>
#include <string>

#include "autil/Log.h"

namespace build_service {
namespace util {
class SwiftClientCreator;
}
} // namespace build_service

namespace swift {
namespace client {
class SwiftReader;
}
} // namespace swift

namespace suez {

struct SwiftSourceConfig {
    std::string swiftRoot;
    std::string topicName;
    bool needFieldFilter = false;
    uint32_t from = 0;
    uint32_t to = 65535;
    std::string clientConfig;
    std::string readerConfig;
    bool topicStreamMode = false;
    std::string topicListStr;
    int64_t startTimestamp = 0;

    bool init(const std::map<std::string, std::string> &params);
    std::string constructReadConfigStr() const;
};

class SwiftReaderCreator {
public:
    static std::unique_ptr<swift::client::SwiftReader>
    create(const std::shared_ptr<build_service::util::SwiftClientCreator> &client,
           const std::map<std::string, std::string> &params);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace suez
