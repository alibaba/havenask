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
#include "suez/drc/SwiftSource.h"

#include <sstream>

#include "autil/Log.h"
#include "build_service/util/SwiftClientCreator.h"
#include "build_service/util/SwiftTopicStreamReaderCreator.h"
#include "suez/drc/DrcConfig.h"
#include "suez/drc/SwiftReaderCreator.h"
#include "swift/client/SwiftReader.h"
#include "swift/protocol/SwiftMessage.pb.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez.drc, SwiftSource);

const std::string SwiftSource::TYPE = "swift";

SwiftSource::SwiftSource(std::unique_ptr<swift::client::SwiftReader> reader) : _swiftReader(std::move(reader)) {}

SwiftSource::~SwiftSource() {}

const std::string &SwiftSource::getType() const { return TYPE; }

bool SwiftSource::seek(int64_t offset) {
    auto ec = _swiftReader->seekByTimestamp(offset, /*force*/ true);
    if (ec != swift::protocol::ERROR_NONE && ec != swift::protocol::ERROR_CLIENT_NO_MORE_MESSAGE) {
        AUTIL_LOG(ERROR, "seekByTimestamp with %ld failed", offset);
        return false;
    }
    return true;
}

bool SwiftSource::read(std::string &data, int64_t &logId) {
    swift::protocol::Message msg;
    int64_t offset;
    auto ec = _swiftReader->read(offset, msg);
    if (ec != swift::protocol::ERROR_NONE) {
        if (ec != swift::protocol::ERROR_CLIENT_NO_MORE_MESSAGE) {
            AUTIL_INTERVAL_LOG(
                1000, ERROR, "read from swift failed, error: %s", swift::protocol::ErrorCode_Name(ec).c_str());
        }
        AUTIL_LOG(DEBUG, "no more message");
        return false;
    }
    logId = msg.timestamp();
    auto messageData = msg.release_data();
    data = std::move(*messageData);
    delete messageData;
    return true;
}

int64_t SwiftSource::getLatestLogId() const {
    auto status = _swiftReader->getSwiftPartitionStatus();
    return status.maxMessageTimestamp;
}

std::unique_ptr<Source> SwiftSource::create(const std::shared_ptr<build_service::util::SwiftClientCreator> &client,
                                            const SourceConfig &config) {
    auto reader = SwiftReaderCreator::create(client, config.parameters);
    if (!reader) {
        AUTIL_LOG(ERROR, "create swift reader failed");
        return {};
    }
    return std::make_unique<SwiftSource>(std::move(reader));
}

} // namespace suez
