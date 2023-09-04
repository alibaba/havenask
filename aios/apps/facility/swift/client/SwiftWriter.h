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
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/SwiftWriterConfig.h"
#include "swift/common/Common.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace common {
class SchemaWriter;
} // namespace common
} // namespace swift

namespace swift {
namespace client {
struct WriterMetrics {
    WriterMetrics() : unsendMsgCount(0), unsendMsgSize(0), uncommittedMsgCount(0), uncommittedMsgSize(0) {}
    size_t unsendMsgCount;
    size_t unsendMsgSize;
    size_t uncommittedMsgCount;
    size_t uncommittedMsgSize;
};

class SwiftWriter {
public:
    SwiftWriter();
    virtual ~SwiftWriter();

private:
    SwiftWriter(const SwiftWriter &);
    SwiftWriter &operator=(const SwiftWriter &);

public:
    virtual protocol::ErrorCode write(MessageInfo &msgInfo) = 0;
    virtual protocol::ErrorCode writes(std::vector<MessageInfo> &msgInfos, bool waitSend = false);
    virtual int64_t getCommittedCheckpointId() const = 0;
    virtual int64_t getLastRefreshTime(uint32_t pid) const = 0;
    virtual std::pair<int32_t, uint16_t>
    getPartitionIdByHashStr(const std::string &zkPath, const std::string &topicName, const std::string &hashStr) = 0;
    virtual std::pair<int32_t, uint16_t> getPartitionIdByHashStr(const std::string &hashStr) = 0;
    virtual void
    getWriterMetrics(const std::string &zkPath, const std::string &topicName, WriterMetrics &writerMetrics) = 0;
    virtual bool isSyncWriter() const = 0;
    virtual bool clearBuffer(int64_t &cpBeg, int64_t &cpEnd) = 0;

    virtual protocol::ErrorCode
    waitFinished(int64_t timeout = SwiftWriterConfig::DEFAULT_WAIT_FINISHED_WRITER_TIME); // us
    virtual protocol::ErrorCode waitSent(int64_t timeout = SwiftWriterConfig::DEFAULT_WAIT_FINISHED_WRITER_TIME);
    virtual void setForceSend(bool forceSend = true);
    virtual common::SchemaWriter *getSchemaWriter() = 0;
    // call setCommittedCallBack before write message
    virtual void
    setCommittedCallBack(const std::function<void(const std::vector<std::pair<int64_t, int64_t>> &)> &func) = 0;
    // write fatal error will callback
    virtual void setErrorCallBack(const std::function<void(const protocol::ErrorCode &)> &func) = 0;

public:
    void setWaitLoopInterval(uint64_t waitLoopInterval) { _waitLoopInterval = waitLoopInterval; }
    virtual bool isFinished() = 0;
    virtual bool isAllSent() = 0;
    virtual std::string getTopicName() { return ""; };

private:
    protocol::ErrorCode wait(const std::function<bool()> &func, int64_t timeout);

protected:
    mutable autil::ThreadMutex _waitMutex;
    volatile bool _forceSend;
    uint64_t _waitLoopInterval;

private:
    friend class SwiftWriterAdapter;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SwiftWriter);

} // namespace client
} // namespace swift
