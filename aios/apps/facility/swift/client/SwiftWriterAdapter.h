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
#include <atomic>
#include <functional>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "swift/client/BufferSizeLimiter.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/Notifier.h"
#include "swift/client/SwiftWriter.h"
#include "swift/client/SwiftWriterConfig.h"
#include "swift/common/Common.h"
#include "swift/common/SchemaWriter.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"

namespace autil {

class ThreadPool;

typedef std::shared_ptr<ThreadPool> ThreadPoolPtr;
} // namespace autil

namespace swift {
namespace network {
class SwiftAdminAdapter;
class SwiftRpcChannelManager;

SWIFT_TYPEDEF_PTR(SwiftAdminAdapter);
SWIFT_TYPEDEF_PTR(SwiftRpcChannelManager);
} // namespace network

namespace util {
class BlockPool;

SWIFT_TYPEDEF_PTR(BlockPool);
} // namespace util

namespace monitor {
class ClientMetricsReporter;
} // namespace monitor

namespace client {
class SwiftWriterImpl;

SWIFT_TYPEDEF_PTR(SwiftWriterImpl);

class SwiftWriterAdapter : public SwiftWriter {
public:
    SwiftWriterAdapter(network::SwiftAdminAdapterPtr adminAdapter,
                       network::SwiftRpcChannelManagerPtr channelManager,
                       autil::ThreadPoolPtr mergeThreadPool,
                       BufferSizeLimiterPtr limiter = BufferSizeLimiterPtr(),
                       util::BlockPoolPtr blockPool = util::BlockPoolPtr(),
                       monitor::ClientMetricsReporter *reporter = nullptr);
    ~SwiftWriterAdapter();

private:
    SwiftWriterAdapter(const SwiftWriterAdapter &);
    SwiftWriterAdapter &operator=(const SwiftWriterAdapter &);

public:
    protocol::ErrorCode init(const SwiftWriterConfig &config);
    protocol::ErrorCode write(MessageInfo &msgInfo) override;
    int64_t getLastRefreshTime(uint32_t pid) const override;
    int64_t getCommittedCheckpointId() const override;

    std::pair<int32_t, uint16_t> getPartitionIdByHashStr(const std::string &zkPath,
                                                         const std::string &topicName,
                                                         const std::string &hashStr) override;
    std::pair<int32_t, uint16_t> getPartitionIdByHashStr(const std::string &hashStr) override;
    void
    getWriterMetrics(const std::string &zkPath, const std::string &topicName, WriterMetrics &writerMetric) override;

    bool isSyncWriter() const override;
    bool clearBuffer(int64_t &cpBeg, int64_t &cpEnd) override;
    void setForceSend(bool forceSend) override;
    common::SchemaWriter *getSchemaWriter() override;
    void
    setCommittedCallBack(const std::function<void(const std::vector<std::pair<int64_t, int64_t>> &)> &func) override;
    void setErrorCallBack(const std::function<void(const protocol::ErrorCode &)> &func) override;

    bool isFinished() override;
    bool isAllSent() override;
    std::string getTopicName() override;

private:
    protocol::ErrorCode createAndInitWriterImpl(const protocol::TopicInfo &tinfo, SwiftWriterImplPtr &retWriterImpl);
    protocol::ErrorCode createNextPhysicWriterImpl(SwiftWriterImplPtr &retWriterImpl);
    bool startSendRequestThread();
    void sendRequestLoop();
    bool doSwitch();
    protocol::ErrorCode doTopicChanged();
    bool checkAuthority(const protocol::TopicInfo &ti);

private:
    SwiftWriterImplPtr _writerImpl;
    autil::LoopThreadPtr _sendRequestThreadPtr;
    network::SwiftAdminAdapterPtr _adminAdapter;
    network::SwiftRpcChannelManagerPtr _channelManager;
    autil::ThreadPoolPtr _mergeThreadPool;
    BufferSizeLimiterPtr _limiter;
    util::BlockPoolPtr _blockPool;
    SwiftWriterConfig _config;
    protocol::TopicInfo _topicInfo;
    mutable autil::ReadWriteLock _switchLock;
    std::atomic<bool> _isSwitching;
    Notifier _switchNotifier;
    autil::ThreadMutex _doTopicChangeLock;
    std::function<void(const std::vector<std::pair<int64_t, int64_t>> &)> _committedFunc;
    std::function<void(const protocol::ErrorCode &)> _errorFunc;
    monitor::ClientMetricsReporter *_reporter;
    std::atomic<int64_t> _writeTime;
    int64_t _finishedTime;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SwiftWriterAdapter);

} // namespace client
} // namespace swift
