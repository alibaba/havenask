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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/NoCopyable.h"
#include "autil/result/Result.h"

namespace build_service::util {
class SwiftClientCreator;
}

namespace suez {

class WALConfig;
using SwiftClientCreatorPtr = std::shared_ptr<build_service::util::SwiftClientCreator>;

class WALStrategy : autil::NoCopyable {
public:
    WALStrategy() = default;
    virtual ~WALStrategy() = default;
    typedef const std::function<void(autil::Result<std::vector<int64_t>>)> CallbackType;

public:
    virtual void log(const std::vector<std::pair<uint16_t, std::string>> &str, CallbackType done) = 0;
    virtual void stop(){};
    virtual void flush() = 0;

public:
    static std::unique_ptr<WALStrategy> create(const WALConfig &config,
                                               const SwiftClientCreatorPtr &swiftClientCreator);
    static bool needSink(const std::string &strategyType);
};

} // namespace suez
