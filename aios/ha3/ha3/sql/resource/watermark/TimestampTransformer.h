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

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/sql/common/GenericWaiter.h"
#include "swift/client/SwiftPartitionStatus.h"

namespace swift::client {
class SwiftReader;
} // namespace swift::client

namespace build_service::util {
class SwiftClientCreator;
} // namespace build_service::util

namespace build_service::workflow {
class RealtimeInfoWrapper;
} // namespace build_service::workflow

namespace autil {
class LoopThread;
} // namespace autil

namespace isearch {
namespace sql {

struct TabletWaiterInitOption;
class TimestampTransformer;

struct TimestampTransformerPack {
    using ClassT = TimestampTransformer;
    using StatusT = swift::client::SwiftPartitionStatus;
    using CallbackParamT = int64_t;
};

class TimestampTransformer final : public GenericWaiter<TimestampTransformerPack>
{
public:
    TimestampTransformer(
        std::shared_ptr<build_service::util::SwiftClientCreator> swiftClientCreator,
        const TabletWaiterInitOption &option);
    ~TimestampTransformer() override;
    TimestampTransformer(const TimestampTransformer &) = delete;
    TimestampTransformer &operator=(const TimestampTransformer &) = delete;

public:
    std::string getDesc() const override;

private:
    bool doInit() override;
    void doStop() override;
    std::unique_ptr<swift::client::SwiftReader> tryCreateReader();
    void getSwiftStatusOnce();
    void getSwiftStatusLoop();
    void onAwake() override;

public: // impl for base class
    swift::client::SwiftPartitionStatus getCurrentStatusImpl() const;
    static int64_t transToKeyImpl(const swift::client::SwiftPartitionStatus &status);
    static int64_t transToCallbackParamImpl(const swift::client::SwiftPartitionStatus &status);

private:
    std::shared_ptr<build_service::util::SwiftClientCreator> _swiftClientCreator;
    swift::client::SwiftPartitionStatus _status;
    std::map<std::string, std::string> _configMap;
    std::unique_ptr<swift::client::SwiftReader> _reader;
    autil::Notifier _swiftReaderNotifier;
    std::shared_ptr<autil::Thread> _getStatusThread;
    mutable autil::ReadWriteLock _statusLock;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace sql
} // namespace isearch
