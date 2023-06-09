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
#include "ha3/sql/resource/watermark/TimestampTransformer.h"

#include "autil/LoopThread.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/util/SwiftClientCreator.h"
#include "ha3/sql/common/GenericWaiter.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/resource/watermark/TabletWaiterInitOption.h"
#include "suez/drc/SwiftSource.h"
#include "swift/client/SwiftReader.h"
#include "swift/protocol/SwiftMessage.pb.h"

using namespace std;
using namespace build_service;
using namespace build_service::config;

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, TimestampTransformer);

TimestampTransformer::TimestampTransformer(
    std::shared_ptr<build_service::util::SwiftClientCreator> swiftClientCreator,
    const TabletWaiterInitOption &option)
    : _swiftClientCreator(swiftClientCreator)
    , _status({0, 0, 0})
    , _swiftReaderNotifier(1) {
    _configMap = option.realtimeInfo.getKvMap();
    _configMap["from"] = std::to_string(option.from);
    _configMap["to"] = std::to_string(option.to);
}

TimestampTransformer::~TimestampTransformer() {
    stop();
}

std::string TimestampTransformer::getDesc() const {
    return autil::StringUtil::toString(_configMap);
}

std::unique_ptr<swift::client::SwiftReader> TimestampTransformer::tryCreateReader() {
    auto reader = suez::SwiftSource::createSwiftReader(_swiftClientCreator, _configMap);
    if (!reader) {
        SQL_LOG(ERROR,
                "[%p] create swift reader with config failed, desc[%s]",
                this,
                getDesc().c_str());
        return nullptr;
    }
    return reader;
}

void TimestampTransformer::getSwiftStatusOnce() {
    if (_reader) {
        auto status = _reader->getSwiftPartitionStatus();
        SQL_LOG(DEBUG,
                "[%p] get swift status refresh time [%ld], max message time [%ld]",
                this,
                status.refreshTime,
                status.maxMessageTimestamp);
        {
            autil::ScopedWriteLock _(_statusLock);
            _status = std::move(status);
        }
        notifyWorkerCheckLoop();
    } else {
        auto reader = tryCreateReader();
        if (!reader) {
            constexpr int64_t sleepInterval = 500; // 500ms
            SQL_LOG(ERROR,
                    "[%p] create swift reader failed, will retry, desc [%s], will sleep [%ld]ms",
                    this,
                    getDesc().c_str(),
                    sleepInterval);
            usleep(sleepInterval * 1000);
        } else {
            _reader = std::move(reader);
        }
    }
}

void TimestampTransformer::getSwiftStatusLoop() {
    while (true) {
        int64_t waitTimeUs = getPendingItemCount() > 0 ? 10 * 1000 : 50 * 1000;
        if (_swiftReaderNotifier.waitNotification(waitTimeUs) == autil::Notifier::EXITED) {
            break;
        }
        getSwiftStatusOnce();
    }
}

void TimestampTransformer::onAwake() {
    _swiftReaderNotifier.notify();
}

bool TimestampTransformer::doInit() {
    getSwiftStatusOnce();
    _getStatusThread = autil::Thread::createThread(
        std::bind(&TimestampTransformer::getSwiftStatusLoop, this), "TsTransGetSt");
    if (!_getStatusThread) {
        SQL_LOG(ERROR, "[%p] create get swift status thread loop failed, desc [%s]", this, getDesc().c_str());
        return false;
    }
    return true;
}

void TimestampTransformer::doStop() {
    _swiftReaderNotifier.notifyExit();
    _getStatusThread.reset();
}

swift::client::SwiftPartitionStatus TimestampTransformer::getCurrentStatusImpl() const {
    autil::ScopedReadLock _(_statusLock);
    return _status;
}

int64_t TimestampTransformer::transToKeyImpl(const swift::client::SwiftPartitionStatus &status) {
    return status.refreshTime + 1;
}

int64_t
TimestampTransformer::transToCallbackParamImpl(const swift::client::SwiftPartitionStatus &status) {
    return status.maxMessageTimestamp;
}

} // namespace sql
} // namespace isearch
