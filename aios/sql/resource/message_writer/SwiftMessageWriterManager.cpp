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
#include "sql/resource/SwiftMessageWriterManager.h"

#include <iosfwd>
#include <map>
#include <utility>

#include "autil/legacy/legacy_jsonizable.h"
#include "sql/common/Log.h"
#include "sql/config/SwiftWriteConfig.h"
#include "swift/client/SwiftWriter.h"
#include "swift/client/helper/CheckpointBuffer.h"
#include "swift/client/helper/SwiftWriterAsyncHelper.h"
#include "swift/protocol/ErrCode.pb.h"

namespace sql {
class MessageWriter;
} // namespace sql

using namespace std;
using namespace swift::client;

namespace sql {
AUTIL_LOG_SETUP(sql, SwiftMessageWriterManager);

SwiftMessageWriterManager::SwiftMessageWriterManager() {}

SwiftMessageWriterManager::~SwiftMessageWriterManager() {
    reset();
}

bool SwiftMessageWriterManager::init(const std::string &config) {
    SwiftWriteConfig swiftWriteConfig;
    try {
        FastFromJsonString(swiftWriteConfig, config);
    } catch (...) {
        SQL_LOG(WARN, "parse swift write config failed [%s].", config.c_str());
        return false;
    }
    createSwiftClient();
    auto ec = _swiftClient->initByConfigStr(swiftWriteConfig.getSwiftClientConfig());
    if (swift::protocol::ERROR_NONE != ec) {
        SQL_LOG(WARN,
                "init swift client failed, config is[%s], ec[%d]",
                swiftWriteConfig.getSwiftClientConfig().c_str(),
                ec);
        reset();
        return false;
    }
    const auto &readWriteConfigMap = swiftWriteConfig.getTableReadWriteConfigMap();
    for (const auto &iter : readWriteConfigMap) {
        const auto &readWriteConfig = iter.second;
        std::unique_ptr<SwiftWriter> swiftWriter(_swiftClient->createWriter(
            readWriteConfig.swiftTopicName, readWriteConfig.swiftWriterConfig));
        if (!swiftWriter) {
            SQL_LOG(WARN,
                    "init swift writer failed, table is [%s], config is [%s]",
                    iter.first.c_str(),
                    FastToJsonString(readWriteConfig, true).c_str());
            reset();
            return false;
        }
        auto swiftWriterAsyncHelper = make_shared<SwiftWriterAsyncHelper>();
        if (!swiftWriterAsyncHelper->init(std::move(swiftWriter), "")) {
            SQL_LOG(
                WARN, "init swift writer async helper failed, table is [%s]", iter.first.c_str());
            reset();
            return false;
        }
        auto swiftMessageWriter = make_shared<SwiftMessageWriter>(swiftWriterAsyncHelper);
        _swiftWriterMap.emplace(iter.first, std::move(swiftMessageWriter));
    }
    SQL_LOG(INFO, "init swift manager success, config is [%s]", config.c_str());
    return true;
}

void SwiftMessageWriterManager::createSwiftClient() {
    _swiftClient.reset(new SwiftClient());
}

MessageWriter *SwiftMessageWriterManager::getMessageWriter(const std::string &dbName,
                                                           const std::string &tableName) {
    auto iter = _swiftWriterMap.find(tableName);
    if (iter != _swiftWriterMap.end()) {
        return iter->second.get();
    }
    return nullptr;
}

void SwiftMessageWriterManager::reset() {
    _swiftWriterMap.clear();
    _swiftClient.reset();
}

} // namespace sql
