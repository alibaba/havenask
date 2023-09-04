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
#include <map>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/HashFunctionBase.h"
#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/ThreadPool.h"
#include "swift/client/BufferSizeLimiter.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/RangeUtil.h"
#include "swift/client/SwiftWriter.h"
#include "swift/client/SwiftWriterConfig.h"
#include "swift/common/Common.h"
#include "swift/common/SchemaWriter.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/BlockPool.h"

namespace swift {

namespace common {
class MessageInfo;
} // namespace common

namespace protocol {
class TopicInfo;
} // namespace protocol

namespace monitor {
class ClientMetricsReporter;
} // namespace monitor

namespace client {
class SingleSwiftWriter;

class SwiftWriterImpl : public SwiftWriter {
public:
    static const std::string functionSeperator;
    static const std::string functionParamSeperator;
    static const std::string funcHashId2Pid;
    static const std::string funcHashIdAsPid;
    static const std::string funcHashStr2HashId_HashFuncPrefix;
    static const std::string funcHashStr2HashId_DefaultHash;
    static const std::string funcHashStr2HashId_GalaxyHash;
    static const std::string funcHashStr2HashId_Hash64;
    typedef bool (SwiftWriterImpl::*ProcessFunc)(MessageInfo &);

public:
    SwiftWriterImpl(network::SwiftAdminAdapterPtr adminAdapter,
                    network::SwiftRpcChannelManagerPtr channelManager,
                    autil::ThreadPoolPtr mergeThreadPool,
                    const protocol::TopicInfo &topicInfo,
                    BufferSizeLimiterPtr limiter = BufferSizeLimiterPtr(),
                    std::string logicTopicName = std::string(),
                    monitor::ClientMetricsReporter *reporter = nullptr);
    // for calc hash and part id
    SwiftWriterImpl(const char *hashFunc, uint32_t topicPartCnt);
    ~SwiftWriterImpl();

private:
    SwiftWriterImpl(const SwiftWriterImpl &);
    SwiftWriterImpl &operator=(const SwiftWriterImpl &);

public:
    protocol::ErrorCode init(const SwiftWriterConfig &config);
    virtual protocol::ErrorCode init(const SwiftWriterConfig &config, util::BlockPoolPtr blockPool);

public:
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
    bool stealUnsendMessage(std::vector<common::MessageInfo> &msgInfoVec);
    void
    setCommittedCallBack(const std::function<void(const std::vector<std::pair<int64_t, int64_t>> &)> &func) override;
    void setErrorCallBack(const std::function<void(const protocol::ErrorCode &)> &func) override;

public:
    protocol::ErrorCode setTopicChanged(int64_t topicVersion);
    void doTopicChanged(uint32_t partCnt, uint32_t rangeCntInPart);
    bool initProcessFuncs();
    void sendRequest(bool &hasSealError);
    protocol::ErrorCode importMsg(MessageInfo &msgInfo);
    std::string getTopicName() override { return _config.topicName; }
    bool topicChanged() const { return _topicChanged; }
    int64_t getTopicVersion() const { return _topicVersion; }

public:
    common::SchemaWriter *getSchemaWriter() override;
    bool isFinished() override;
    bool isAllSent() override;

protected:
    // virtual for test
    virtual SingleSwiftWriter *insertSingleWriter(uint32_t pid);

protected:
    SingleSwiftWriter *getSingleWriter(uint32_t pid);

private:
    protocol::ErrorCode writeMsg(MessageInfo &msgInfo);
    void clear();
    void doSendRequestLoop(int64_t now, bool force);
    void setCheckpointId(int64_t checkpointId);
    bool processHashId2PartId(MessageInfo &msgInfo);
    bool processHashIdAsPartId(MessageInfo &msgInfo);
    bool processHashStr(MessageInfo &msgInfo);
    protocol::ErrorCode validateMessageInfo(const MessageInfo &msgInfo);
    bool parseFunctionParam(const std::string &funStr, std::string &funName, uint32_t &partCnt);
    protocol::ErrorCode processMsgInfo(MessageInfo &msgInfo);
    uint32_t getPartitionCount() const { return _partitionCount; }
    autil::HashFunctionBasePtr createHashFunc(const std::string &hashFuncStr);

protected:
    network::SwiftAdminAdapterPtr _adminAdapter;
    network::SwiftRpcChannelManagerPtr _channelManager;
    mutable autil::ReadWriteLock _mapLock;
    std::map<uint32_t, SingleSwiftWriter *> _writers;
    mutable autil::ThreadMutex _checkpointMutex;
    mutable autil::ThreadMutex _waitMutex;
    mutable autil::ReadWriteLock _rangeUtilLock;
    mutable autil::ReadWriteLock _hashFuncLock;
    volatile uint32_t _partitionCount;
    uint32_t _rangeCountInPartition;
    int64_t _checkpointId;
    protocol::ErrorCode _lastErrorCode;
    autil::HashFunctionBasePtr _hashFunctionBasePtr;
    std::string _hashFuncStr;
    RangeUtilPtr _rangeUtil;
    std::vector<ProcessFunc> _processFuncs;
    std::atomic<bool> _topicChanged;
    SwiftWriterConfig _config;
    util::BlockPoolPtr _blockPool;
    autil::ThreadPoolPtr _mergeThreadPool;
    BufferSizeLimiterPtr _limiter;
    std::string _logicTopicName;
    std::atomic<int64_t> _topicVersion;
    std::function<void(const std::vector<std::pair<int64_t, int64_t>> &)> _committedFunc;
    std::function<void(const protocol::ErrorCode &)> _errorFunc;
    int64_t _versionControlPid = -1;
    monitor::ClientMetricsReporter *_reporter;
    std::vector<int64_t> _sessionIds;

private:
    friend class SwiftClientFactory;
    friend class SwiftWriterImplTest;
    friend class FakeClientHelper;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SwiftWriterImpl);

} // namespace client
} // namespace swift
