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

#include "autil/Lock.h"
#include "autil/result/Result.h"
#include "suez/table/wal/WALStrategy.h"
namespace swift::client {
class SwiftClient;
class FixedTimeoutSwiftWriterAsyncHelper;
} // namespace swift::client

namespace build_service::util {
class SwiftClientCreator;
}
namespace suez {
class WALConfig;
using SwiftClientCreatorPtr = std::shared_ptr<build_service::util::SwiftClientCreator>;

class RealtimeSwiftWAL : public WALStrategy {
public:
    RealtimeSwiftWAL();
    ~RealtimeSwiftWAL();

public:
    bool init(const WALConfig &config, const SwiftClientCreatorPtr &swiftClientCreator);
    void log(const std::vector<std::pair<uint16_t, std::string>> &strs, CallbackType done) override;
    void stop() override;
    void flush() override;

private:
    std::shared_ptr<swift::client::FixedTimeoutSwiftWriterAsyncHelper> getSwiftWriterAsyncHelper();

private:
    int64_t _currentId = 0;
    std::string _topicName;
    std::shared_ptr<swift::client::FixedTimeoutSwiftWriterAsyncHelper> _swiftWriterAsyncHelper;
    autil::ReadWriteLock _swiftHelperLock;
};

} // namespace suez
