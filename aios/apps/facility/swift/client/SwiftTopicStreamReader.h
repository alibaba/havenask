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

#include <functional>
#include <limits>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "swift/client/SwiftPartitionStatus.h"
#include "swift/client/SwiftReader.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace common {
class SchemaReader;
} // namespace common
namespace protocol {
class Message;
class Messages;
class ReaderProgress;
} // namespace protocol

namespace client {
class SwiftReaderConfig;

struct SingleTopicReader {
    std::unique_ptr<SwiftReader> impl;
    std::string topicName;
    // the range: [start, end)
    int64_t startTimestamp = 0;
    int64_t stopTimestamp = std::numeric_limits<int64_t>::max();

    bool check() const {
        if (topicName.empty() || !impl) {
            return false;
        }
        return stopTimestamp > startTimestamp;
    }

    bool isBounded() const { return stopTimestamp != std::numeric_limits<int64_t>::max(); }

    SwiftReader *getImpl() { return impl.get(); }

    bool operator<(const SingleTopicReader &other) const {
        return startTimestamp < other.startTimestamp && stopTimestamp < other.stopTimestamp;
    }

    bool contains(int64_t timestamp) const { return startTimestamp <= timestamp && timestamp < stopTimestamp; }
};

class SwiftTopicStreamReader : public SwiftReader {
public:
    SwiftTopicStreamReader();
    ~SwiftTopicStreamReader();

public:
    protocol::ErrorCode init(std::vector<SingleTopicReader> readers);
    protocol::ErrorCode init(const SwiftReaderConfig &config) override;

    protocol::ErrorCode read(protocol::Message &msg, int64_t timeout = 3 * 1000000) override;
    protocol::ErrorCode read(protocol::Messages &msgs, int64_t timeout = 3 * 1000000) override;
    protocol::ErrorCode read(int64_t &timeStamp, protocol::Message &msg, int64_t timeout = 3 * 1000000) override;
    protocol::ErrorCode read(int64_t &timeStamp, protocol::Messages &msgs, int64_t timeout = 3 * 1000000) override;

    protocol::ErrorCode seekByMessageId(int64_t msgId) override;
    protocol::ErrorCode seekByTimestamp(int64_t timestamp, bool force = false) override;
    protocol::ErrorCode seekByProgress(const protocol::ReaderProgress &progress, bool force = false) override;

    void checkCurrentError(protocol::ErrorCode &errorCode, std::string &errorMsg) const override;
    SwiftPartitionStatus getSwiftPartitionStatus() override;

    void setRequiredFieldNames(const std::vector<std::string> &fieldNames) override;
    std::vector<std::string> getRequiredFieldNames() override;
    void setFieldFilterDesc(const std::string &filterDesc) override;

    void setTimestampLimit(int64_t timestampLimit, int64_t &acceptTimestamp) override;
    bool updateCommittedCheckpoint(int64_t checkpoint) override;

    common::SchemaReader *getSchemaReader(const char *data, protocol::ErrorCode &ec) override;
    protocol::ErrorCode getReaderProgress(protocol::ReaderProgress &progress) const override;
    int64_t getCheckpointTimestamp() const override;
    int getCurrentIdx() const { return _currentIdx; }
    std::string getTopicName(size_t idx) const;

private:
    bool checkTopicStream(const std::vector<SingleTopicReader> &readers) const;
    bool moveToNextTopic();
    protocol::ErrorCode readLoop(std::function<protocol::ErrorCode(SwiftReader *)> fn);

private:
    std::vector<SingleTopicReader> _readers;
    int _currentIdx;
    SingleTopicReader *_currentReader;
};

} // namespace client
} // namespace swift
