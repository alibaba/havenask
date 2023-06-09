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
#include "RealtimeSwiftWAL.h"

#include <sstream>

#include "autil/SharedPtrUtil.h"
#include "autil/result/Errors.h"
#include "build_service/common_define.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/util/SwiftClientCreator.h"
#include "suez/table/wal/WALConfig.h"
#include "swift/client/SwiftClient.h"
#include "swift/client/helper/SwiftWriterAsyncHelper.h"

using namespace std;
using namespace autil;
using namespace autil::result;
using namespace build_service;
using namespace build_service::config;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, RealtimeSwiftWAL);

namespace suez {

RealtimeSwiftWAL::RealtimeSwiftWAL() : _currentId(0) {}

RealtimeSwiftWAL::~RealtimeSwiftWAL() { stop(); }

bool RealtimeSwiftWAL::init(const WALConfig &config, const SwiftClientCreatorPtr &swiftClientCreator) {
    _timeoutMs = config.timeoutMs;

    const auto &kvMap = config.sinkDescription;
    if (getValueFromKeyValueMap(kvMap, READ_SRC_TYPE) != SWIFT_READ_SRC) {
        AUTIL_LOG(ERROR, "require last data source is swift in data_table.json");
        return false;
    }
    string zfsRootPath = getValueFromKeyValueMap(kvMap, SWIFT_ZOOKEEPER_ROOT);
    string swiftClientConfig = getValueFromKeyValueMap(kvMap, SWIFT_CLIENT_CONFIG);

    auto swiftClient = swiftClientCreator->createSwiftClient(zfsRootPath, swiftClientConfig);
    if (!swiftClient) {
        AUTIL_LOG(ERROR, "init swift client failed, [%s %s]", zfsRootPath.c_str(), swiftClientConfig.c_str());
        return false;
    }
    string writerConfigStr = getValueFromKeyValueMap(kvMap, SWIFT_WRITER_CONFIG);
    _topicName = getValueFromKeyValueMap(kvMap, SWIFT_TOPIC_NAME);
    writerConfigStr = "topicName=" + _topicName + ";needTimestamp=true;functionChain=hashId2partId;mode=async|safe;" +
                      writerConfigStr;
    swift::protocol::ErrorInfo err;
    std::unique_ptr<swift::client::SwiftWriter> swiftWriter(swiftClient->createWriter(writerConfigStr, &err));
    if (!swiftWriter || err.errcode() != swift::protocol::ERROR_NONE) {
        AUTIL_LOG(ERROR, "create swift writer failed, error [%s]", err.ShortDebugString().c_str());
        return false;
    }

    _swiftWriterAsyncHelper.reset(new swift::client::SwiftWriterAsyncHelper);
    if (!_swiftWriterAsyncHelper->init(std::move(swiftWriter), config.desc)) {
        AUTIL_LOG(ERROR, "init swift writer async helper failed");
        return false;
    }

    return true;
}

void RealtimeSwiftWAL::log(const std::vector<std::pair<uint16_t, string>> &docs, RealtimeSwiftWAL::CallbackType done) {
    auto helper = getSwiftWriterAsyncHelper();
    if (!helper) {
        AUTIL_LOG(ERROR, "swift wal is stopped");
        done(RuntimeError::make("RealtimeSwiftWAL stopped"));
        return;
    }
    vector<swift::common::MessageInfo> messages;
    for (const auto &doc : docs) {
        swift::common::MessageInfo message;
        message.data = doc.second;
        message.uint16Payload = doc.first;
        message.checkpointId = _currentId++;
        messages.push_back(message);
    }
    auto timeoutMs = std::min<int64_t>(_timeoutMs, 60 * 1000);
    auto writeDone = [_done = std::move(done),
                      timeoutMs](autil::result::Result<swift::client::SwiftWriteCallbackParam> result) {
        if (result.is_err()) {
            AUTIL_LOG(ERROR,
                      "swift write failed, timeout [%ld] ms, error [%s]",
                      timeoutMs,
                      result.get_error().message().c_str());
            _done(std::move(result).steal_error());
            return;
        }
        auto param = std::move(result).steal_value();
        std::vector<int64_t> timestamps;
        timestamps.assign(param.tsData, param.tsData + param.tsCount);
        _done(std::move(timestamps));
    };
    helper->write(messages.data(), messages.size(), std::move(writeDone), timeoutMs * 1000);
}

std::shared_ptr<swift::client::SwiftWriterAsyncHelper> RealtimeSwiftWAL::getSwiftWriterAsyncHelper() {
    ScopedReadLock _(_swiftHelperLock);
    return _swiftWriterAsyncHelper;
}

void RealtimeSwiftWAL::stop() {
    std::shared_ptr<swift::client::SwiftWriterAsyncHelper> helper;
    {
        ScopedWriteLock _(_swiftHelperLock);
        helper = std::move(_swiftWriterAsyncHelper);
    }
    auto topicName = helper ? helper->getTopicName() : "";
    AUTIL_LOG(INFO,
              "begin wait use count for swift async helper, topic[%s], current[%lu] expect[1] ptr[%p]",
              topicName.c_str(),
              helper.use_count(),
              helper.get());
    (void)autil::SharedPtrUtil::waitUseCount(helper, 1, 5 * 1000 * 1000); // 5s
    AUTIL_LOG(INFO,
              "end wait use count for swift async helper, topic[%s], current[%lu] expect[1] ptr[%p]",
              topicName.c_str(),
              helper.use_count(),
              helper.get());
    helper.reset();
}

void RealtimeSwiftWAL::flush() {
    auto helper = getSwiftWriterAsyncHelper();
    if (!helper) {
        return;
    }
    helper->waitEmpty();
}

} // namespace suez
