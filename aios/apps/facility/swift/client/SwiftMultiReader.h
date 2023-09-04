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

#include <algorithm>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "swift/client/Notifier.h"
#include "swift/client/SwiftPartitionStatus.h"
#include "swift/client/SwiftReader.h"
#include "swift/client/SwiftReaderAdapter.h"
#include "swift/common/Common.h"
#include "swift/common/SchemaReader.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace protocol {
class Message;
class Messages;
class ReaderProgress;
} // namespace protocol
namespace client {

class SwiftReaderConfig;

class SwiftMultiReader : public SwiftReader {
    enum READ_ORDER {
        RO_DEFAULT = 0,
        RO_SEQ = 1,
    };

public:
    SwiftMultiReader();
    virtual ~SwiftMultiReader();

private:
    SwiftMultiReader(const SwiftMultiReader &);
    SwiftMultiReader &operator=(const SwiftMultiReader &);

public:
    protocol::ErrorCode init(const SwiftReaderConfig &config) override;
    protocol::ErrorCode read(int64_t &timeStamp, protocol::Messages &msgs, int64_t timeout = 3 * 1000000) override;
    protocol::ErrorCode read(int64_t &timeStamp, protocol::Message &msg, int64_t timeout = 3 * 1000000) override;
    protocol::ErrorCode seekByTimestamp(int64_t timeStamp, bool force = false) override;
    protocol::ErrorCode seekByProgress(const protocol::ReaderProgress &progress, bool force = false) override;
    void checkCurrentError(protocol::ErrorCode &errorCode, std::string &errorMsg) const override;
    SwiftPartitionStatus getSwiftPartitionStatus() override;
    void setRequiredFieldNames(const std::vector<std::string> &fieldNames) override;
    void setFieldFilterDesc(const std::string &fieldFilterDesc) override;
    void setTimestampLimit(int64_t timestampLimit, int64_t &acceptTimestamp) override;
    std::vector<std::string> getRequiredFieldNames() override;
    bool updateCommittedCheckpoint(int64_t checkpoint) override;
    protocol::ErrorCode seekByMessageId(int64_t msgId) override;
    common::SchemaReader *getSchemaReader(const char *data, protocol::ErrorCode &ec) override;
    protocol::ErrorCode getReaderProgress(protocol::ReaderProgress &progress) const override;
    int64_t getCheckpointTimestamp() const override;
    bool setReaderOrder(const std::string &order);

public:
    bool addReader(SwiftReader *reader);
    size_t size() { return _readerAdapterVec.size(); }
    Notifier *getNotifier() { return _notifier; }

private:
    void getReaderPos();
    protocol::ErrorCode checkTimestampLimit(protocol::ErrorCode ec);
    bool filterReaderProgress(const protocol::ReaderProgress &progress,
                              const SwiftReaderConfig &config,
                              protocol::ReaderProgress &outProgress);
    template <typename T>
    protocol::ErrorCode readMsg(int64_t &timeStamp,
                                T &msg,
                                int64_t timeout = 3 * 1000000); // 3s
    template <typename T>
    protocol::ErrorCode doRead(int64_t &timeStamp, T &msg);

private:
    std::vector<SwiftReaderAdapter *> _readerAdapterVec;
    std::vector<bool> _exceedTimestampLimitVec;
    std::vector<int64_t> _nextTimestampVec;
    std::vector<int64_t> _checkpointTimestampVec;
    size_t _readerPos;
    int64_t _timestampLimit;
    mutable autil::ReadWriteLock _readerLock;
    READ_ORDER _readerOrder;
    Notifier *_notifier;

private:
    friend class SwiftMultiReaderTest;
    friend class SwiftClientFactory;
    friend class FakeClientHelper;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SwiftMultiReader);

template <typename T>
protocol::ErrorCode SwiftMultiReader::readMsg(int64_t &timeStamp, T &msg, int64_t timeout) {
    protocol::ErrorCode ec = doRead(timeStamp, msg);
    if (protocol::ERROR_CLIENT_NO_MORE_MESSAGE != ec) {
        return ec;
    }
    if (timeout <= 0) {
        AUTIL_LOG(INFO, "timeout[%ld] adjust to 3s", timeout);
        timeout = 3000000;
    }
    int64_t interval = timeout;
    int64_t endTime = autil::TimeUtility::currentTime() + timeout;
    while (interval > 0) {
        _notifier->setNeedNotify(true);
        ec = doRead(timeStamp, msg);
        if (protocol::ERROR_NONE == ec) {
            break;
        }
        _notifier->wait(interval);
        int64_t currentTime = autil::TimeUtility::currentTime();
        interval = endTime - currentTime;
    }
    _notifier->setNeedNotify(false);
    return ec;
}

template <typename T>
protocol::ErrorCode SwiftMultiReader::doRead(int64_t &timeStamp, T &msg) {
    int64_t timeLocal = 0;
    protocol::ErrorCode ec = protocol::ERROR_NONE;
    for (size_t idx = 0; idx < _readerAdapterVec.size(); ++idx) {
        getReaderPos();
        ec = _readerAdapterVec[_readerPos]->read(timeLocal, msg, 0);
        if (protocol::ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT == ec) {
            _exceedTimestampLimitVec[_readerPos] = true;
            timeLocal = std::max(timeLocal, _timestampLimit + 1);
        } else {
            _exceedTimestampLimitVec[_readerPos] = false;
        }
        _nextTimestampVec[_readerPos] = _readerAdapterVec[_readerPos]->getNextMsgTimestamp();
        _checkpointTimestampVec[_readerPos] = timeLocal;
        timeStamp = getCheckpointTimestamp();
        if (protocol::ERROR_NONE == ec) {
            break;
        }
    }
    return checkTimestampLimit(ec);
}

} // namespace client
} // namespace swift
