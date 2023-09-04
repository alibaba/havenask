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

#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/ThreadPool.h"
#include "swift/client/SwiftClientConfig.h"
#include "swift/client/SwiftReader.h"       //IWYU pragma: keep
#include "swift/client/SwiftReaderConfig.h" //IWYU pragma: keep
#include "swift/client/SwiftWriter.h"
#include "swift/client/SwiftWriterConfig.h" //IWYU pragma: keep
#include "swift/common/Common.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"

namespace arpc {
class ANetRPCChannelManager;
}
namespace kmonitor {
class MetricsReporter;

typedef std::shared_ptr<MetricsReporter> MetricsReporterPtr;
} // namespace kmonitor

namespace swift {

namespace util {
class BlockPool;

typedef std::shared_ptr<BlockPool> BlockPoolPtr;
} // namespace util
namespace monitor {
class ClientMetricsReporter;

typedef std::shared_ptr<ClientMetricsReporter> ClientMetricsReporterPtr;
} // namespace monitor
} // namespace swift

namespace swift {
namespace client {
class BufferSizeLimiter;
class Notifier;
class SwiftMultiReader;

typedef std::shared_ptr<BufferSizeLimiter> BufferSizeLimiterPtr;

class SwiftClient {
public:
    SwiftClient(const kmonitor::MetricsReporterPtr &reporter = kmonitor::MetricsReporterPtr());
    virtual ~SwiftClient();

private:
    SwiftClient(const SwiftClient &);
    SwiftClient &operator=(const SwiftClient &);

public:
    // new interface from swift 1.0
    virtual protocol::ErrorCode initByConfigStr(const std::string &configStr);
    virtual protocol::ErrorCode initByConfigStr(const std::string &configStr,
                                                arpc::ANetRPCChannelManager *channelManager);

    virtual SwiftReader *createReader(const std::string &readerConfigStr, protocol::ErrorInfo *errorCode);

    virtual SwiftWriter *createWriter(const std::string &writerConfigStr, protocol::ErrorInfo *errorCode);

public:
    virtual protocol::ErrorCode init(const SwiftClientConfig &config);
    // old interface

    virtual protocol::ErrorCode init(const std::string &zkRootPath,
                                     arpc::ANetRPCChannelManager *channelManager = NULL,
                                     const SwiftClientConfig &config = SwiftClientConfig()) __attribute__((deprecated));

    virtual SwiftReader *createReader(const std::string &topicName,
                                      const std::vector<uint32_t> &partitionId,
                                      const protocol::Filter &filter = protocol::Filter(),
                                      const std::string &readerConfigStr = "",
                                      protocol::ErrorInfo *errorInfo = NULL) __attribute__((deprecated));

    virtual SwiftReader *createReader(const std::string &topicName,
                                      const protocol::Filter &filter = protocol::Filter(),
                                      const std::string &readerConfigStr = "",
                                      protocol::ErrorInfo *errorInfo = NULL) __attribute__((deprecated));

    virtual SwiftWriter *createWriter(const std::string &topicName,
                                      const std::string &writerConfigStr = "",
                                      protocol::ErrorInfo *errorInfo = NULL) __attribute__((deprecated));

    network::SwiftAdminAdapterPtr getAdminAdapter();
    network::SwiftAdminAdapterPtr getAdminAdapter(const std::string &zkPath);

    static bool initAlog(const std::string &content);

public:
    // for test
    void resetAdminAdapter(network::SwiftAdminAdapterPtr adminAdapter, const std::string &zkPath = "") {
        _adminAdapterMap[zkPath] = adminAdapter;
    }

protected:
    virtual protocol::ErrorCode initLog(const std::string &alogConf);

    virtual SwiftReader *
    doCreateReader(SwiftReaderConfig config, protocol::ErrorInfo *errorInfo, Notifier *notifier = NULL);

    virtual SwiftWriter *doCreateWriter(const std::vector<SwiftWriterConfig> &configVec,
                                        protocol::ErrorInfo *errorInfo);

    protocol::ErrorCode init(const std::vector<SwiftClientConfig> &configVec,
                             arpc::ANetRPCChannelManager *channelManager);
    SwiftWriter *doCreateSingleWriter(const SwiftWriterConfig &config,
                                      network::SwiftAdminAdapterPtr adminAdapter,
                                      util::BlockPoolPtr blockPool,
                                      BufferSizeLimiterPtr limiter,
                                      protocol::ErrorInfo *errorInfo);
    void parseRetryTimesAndInterval();
    void clear();
    virtual network::SwiftAdminAdapterPtr createSwiftAdminAdapter(const SwiftClientConfig &config,
                                                                  network::SwiftRpcChannelManagerPtr channelManager);
    util::BlockPoolPtr getBlockPool(const std::string &zkPath);
    BufferSizeLimiterPtr getLimiter(const std::string &zkPath);
    size_t getPhyMemSize();
    SwiftWriterPtr createTracingWriter(const std::string &zkPath);

private:
    bool adjustClientConfigHack(std::vector<SwiftClientConfig> &clientConfigVec);
    bool adjustReaderConfigHack(std::vector<SwiftReaderConfig> &readerConfigVec);
    SwiftMultiReader *doCreateMultiReader(const std::vector<SwiftReaderConfig> &configVec,
                                          protocol::ErrorInfo *errorInfo);
    bool initTraceFileAlogger();

protected:
    std::map<std::string, network::SwiftAdminAdapterPtr> _adminAdapterMap;
    std::map<std::string, util::BlockPoolPtr> _blockPoolMap;
    std::map<std::string, BufferSizeLimiterPtr> _limiterMap;
    std::vector<SwiftClientConfig> _configVec;
    uint32_t _retryTimes;
    int64_t _retryTimeInterval;
    autil::ThreadPoolPtr _mergeThreadPool;
    network::SwiftRpcChannelManagerPtr _channelManager;
    std::map<std::string, std::pair<bool, SwiftWriterPtr>> _tracingMsgInfoMap;
    std::map<std::string, SwiftWriterPtr> _tracingErrorResponseMap;
    monitor::ClientMetricsReporterPtr _metricsReporter;

public:
    static bool traceFlag;

private:
    std::vector<std::string> _clientZkPaths;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SwiftClient);

} // namespace client
} // namespace swift
