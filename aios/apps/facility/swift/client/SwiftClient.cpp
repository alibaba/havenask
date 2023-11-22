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
#include "swift/client/SwiftClient.h"

#include <algorithm>
#include <cstdint>
#include <exception>
#include <ext/alloc_traits.h>
#include <iostream>
#include <linux/sysinfo.h>
#include <stdio.h>
#include <unistd.h>

#include "alog/Configurator.h"
#include "autil/CommonMacros.h"
#include "autil/HashAlgorithm.h"
#include "autil/Lock.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "kmonitor/client/MetricsReporter.h"
#include "swift/client/BufferSizeLimiter.h"
#include "swift/client/SwiftMultiReader.h"
#include "swift/client/SwiftReader.h"
#include "swift/client/SwiftReaderAdapter.h"
#include "swift/client/SwiftReaderConfig.h"
#include "swift/client/SwiftWriterAdapter.h"
#include "swift/client/SwiftWriterConfig.h"
#include "swift/client/trace/ReadMessageTracer.h"
#include "swift/client/trace/SwiftErrorResponseCollector.h"
#include "swift/client/trace/TraceFileLogger.h"
#include "swift/client/trace/TraceLogger.h"
#include "swift/client/trace/TraceSwiftLogger.h"
#include "swift/config/ClientAuthorizerInfo.h"
#include "swift/monitor/ClientMetricsReporter.h"
#include "swift/network/ClientFileUtil.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageWriter.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/TraceMessage.pb.h"
#include "swift/util/BlockPool.h"
#include "swift/util/IpUtil.h"
#include "sys/sysinfo.h"

using namespace std;
using namespace autil;
using namespace alog;

using namespace swift::protocol;
using namespace swift::util;
using namespace swift::network;
using namespace swift::monitor;
using namespace swift::auth;

namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, SwiftClient);

const string CLIENT_CONFIG_MULTI_READER_SEPERATOR = "#";

bool SwiftClient::traceFlag = false;

SwiftClient::SwiftClient(const kmonitor::MetricsReporterPtr &reporter)
    : _retryTimes(SwiftClientConfig::MIN_TRY_TIMES)
    , _retryTimeInterval(SwiftClientConfig::MIN_RETRY_TIME_INTERVAL)
    , _metricsReporter(nullptr) {
    if (reporter) {
        _metricsReporter.reset(new ClientMetricsReporter(reporter));
    }
}

SwiftClient::~SwiftClient() {
    _mergeThreadPool.reset();
    clear();
}

void SwiftClient::clear() {
    ResponseCollectorSingleton::getInstance()->resetSwiftWriter();
    _tracingMsgInfoMap.clear();
    _tracingErrorResponseMap.clear();
    _adminAdapterMap.clear();
    _blockPoolMap.clear();
    _limiterMap.clear();
    _channelManager.reset();
    _configVec.clear();
}

#define CREATE_WITH_FILTER_ERROR_CHECK(swiftFilter)                                                                    \
    if (swiftFilter.from() > swiftFilter.to()) {                                                                       \
        AUTIL_LOG(ERROR, "Invalid Filter : [%s]", swiftFilter.ShortDebugString().c_str());                             \
        if (errorInfo) {                                                                                               \
            errorInfo->set_errcode(ERROR_CLIENT_INVALID_PARAMETERS);                                                   \
        }                                                                                                              \
        return NULL;                                                                                                   \
    }

#define CHECK_ERROR_CODE(p)                                                                                            \
    string configStr = autil::legacy::ToJsonString(config);                                                            \
    if (ERROR_NONE != ec) {                                                                                            \
        if (errorInfo) {                                                                                               \
            errorInfo->set_errcode(ec);                                                                                \
        }                                                                                                              \
        delete p;                                                                                                      \
        AUTIL_LOG(WARN, "Init failed, config json [%s]", configStr.c_str());                                           \
        return NULL;                                                                                                   \
    }                                                                                                                  \
    AUTIL_LOG(INFO, "Init success, config json [%s]", configStr.c_str());

#define RETRY_MODE_BEGIN                                                                                               \
    ErrorCode ec = ERROR_NONE;                                                                                         \
    for (uint32_t j = 0; j < _retryTimes; ++j) {

#define RETRY_MODE_END                                                                                                 \
    if (ec == ERROR_NONE || ec == ERROR_ADMIN_TOPIC_NOT_EXISTED) {                                                     \
        break;                                                                                                         \
    } else {                                                                                                           \
        usleep(_retryTimeInterval);                                                                                    \
    }                                                                                                                  \
    }

protocol::ErrorCode SwiftClient::initLog(const std::string &alogConf) {
    static autil::ThreadMutex mutex;
    static bool isInited = false;
    autil::ScopedLock lock(mutex);
    if (isInited) {
        return ERROR_NONE;
    }
    if (alogConf.empty()) {
        return ERROR_NONE;
    }
    string content;
    ClientFileUtil fileUtil;
    if (!fileUtil.readFile(alogConf, content)) {
        fprintf(stderr, "read log config file[%s] failed.", alogConf.c_str());
        return ERROR_UNKNOWN;
    }
    if (!initAlog(content)) {
        return ERROR_UNKNOWN;
    }
    isInited = true;
    return ERROR_NONE;
}

protocol::ErrorCode SwiftClient::initByConfigStr(const std::string &configStr) {
    return initByConfigStr(configStr, NULL);
}

protocol::ErrorCode SwiftClient::initByConfigStr(const std::string &configStr,
                                                 arpc::ANetRPCChannelManager *channelManager) {

    vector<SwiftClientConfig> clientConfigVec;
    try {
        autil::legacy::FromJsonString(clientConfigVec, configStr);
    } catch (const autil::legacy::ExceptionBase &e) {}
    if (0 == clientConfigVec.size()) {
        // split multi client config
        const vector<string> &configStrVec =
            StringUtil::split(configStr, SwiftClientConfig::CLIENT_CONFIG_MULTI_SEPERATOR);
        for (size_t i = 0; i < configStrVec.size(); i++) {
            SwiftClientConfig clientConfig;
            if (!clientConfig.parseFromString(configStrVec[i])) {
                fprintf(stderr, "parse client config string failed [%s]!\n", configStrVec[i].c_str());
                return ERROR_UNKNOWN;
            }
            clientConfigVec.push_back(clientConfig);
        }
    }
    if (1 == clientConfigVec.size()) {
        adjustClientConfigHack(clientConfigVec);
    }
    return init(clientConfigVec, channelManager);
}

bool SwiftClient::adjustClientConfigHack(vector<SwiftClientConfig> &clientConfigVec) {
    _clientZkPaths = StringUtil::split(clientConfigVec[0].zkPath, CLIENT_CONFIG_MULTI_READER_SEPERATOR);
    if (1 == _clientZkPaths.size()) {
        _clientZkPaths.clear();
        return true;
    }
    for (size_t idx = 1; idx < _clientZkPaths.size(); ++idx) {
        clientConfigVec[0].zkPath = _clientZkPaths[idx];
        clientConfigVec.push_back(clientConfigVec[0]);
    }
    clientConfigVec[0].zkPath = _clientZkPaths[0];
    return true;
}

protocol::ErrorCode SwiftClient::init(const SwiftClientConfig &config) {
    vector<SwiftClientConfig> clientConfigVec;
    clientConfigVec.push_back(config);
    return init(clientConfigVec, NULL);
}

ErrorCode SwiftClient::init(const string &zkRootPath,
                            arpc::ANetRPCChannelManager *channelManager,
                            const SwiftClientConfig &config) {
    SwiftClientConfig clientConfig = config;
    clientConfig.zkPath = zkRootPath;
    vector<SwiftClientConfig> clientConfigVec;
    clientConfigVec.push_back(clientConfig);
    return init(clientConfigVec, channelManager);
}

ErrorCode SwiftClient::init(const vector<SwiftClientConfig> &configVec, arpc::ANetRPCChannelManager *channelManager) {
    clear();
    if (configVec.size() == 0) {
        return ERROR_CLIENT_INIT_INVALID_PARAMETERS;
    }
    _configVec = configVec;
    _channelManager.reset(
        new SwiftRpcChannelManager(configVec[0].ioThreadNum, configVec[0].rpcSendQueueSize, configVec[0].rpcTimeout));
    if (!_channelManager->init(channelManager)) {
        AUTIL_LOG(WARN, "init swift rpc channel manager  failed.");
        return ERROR_UNKNOWN;
    }
    parseRetryTimesAndInterval();
    uint32_t threadPoolSize = 0;
    for (size_t i = 0; i < _configVec.size(); i++) {
        string configStr = autil::legacy::ToJsonString(_configVec[i]);
        if (_configVec[i].zkPath.empty()) {
            AUTIL_LOG(WARN, "Init swift client failed, zkPath is empty, config json [%s]", configStr.c_str());
            return ERROR_CLIENT_INIT_INVALID_PARAMETERS;
        }
        ErrorCode initEc = initLog(_configVec[i].logConfigFile);
        if (ERROR_NONE != initEc) {
            AUTIL_LOG(WARN, "Init swift client failed ec[%d], config json [%s]", initEc, configStr.c_str());
            return initEc;
        }
        if (_adminAdapterMap.find(_configVec[i].zkPath) != _adminAdapterMap.end()) {
            continue;
        }
        SwiftAdminAdapterPtr adminAdapter = createSwiftAdminAdapter(_configVec[i], _channelManager);
        RETRY_MODE_BEGIN;
        ec = adminAdapter->createAdminClient();
        RETRY_MODE_END;
        if (ec != ERROR_NONE) {
            adminAdapter.reset();
            AUTIL_LOG(WARN, "create admin client failed ec[%d], config json [%s]", ec, configStr.c_str());
            return ec;
        }
        _adminAdapterMap[_configVec[i].zkPath] = adminAdapter;
        int64_t maxBufferSize = _configVec[i].maxWriteBufferSizeMb * 1024 * 1024;
        if (maxBufferSize <= 0) {
            maxBufferSize = getPhyMemSize() * 0.8;
        }
        int64_t bufferSize = maxBufferSize;
        if (_configVec[i].tempWriteBufferPercent < 1.0) {
            bufferSize = maxBufferSize * (1 - _configVec[i].tempWriteBufferPercent);
        }
        BlockPoolPtr blockPoolPtr(new BlockPool(bufferSize, _configVec[i].bufferBlockSizeKb * 1024));
        _blockPoolMap[_configVec[i].zkPath] = blockPoolPtr;
        int64_t bufferSizeLimit = _configVec[i].tempWriteBufferPercent * 1024 * 1024;
        if (_configVec[i].tempWriteBufferPercent < 1.0) {
            bufferSizeLimit = maxBufferSize * _configVec[i].tempWriteBufferPercent * 1024 * 1024;
            AUTIL_LOG(INFO, "addjust buffer size limit to [%ld]", bufferSizeLimit);
        }
        BufferSizeLimiterPtr limiter(new BufferSizeLimiter(bufferSizeLimit));
        _limiterMap[_configVec[i].zkPath] = limiter;
        AUTIL_LOG(INFO, "Init swift client success, config json [%s]", configStr.c_str());
        if (threadPoolSize < _configVec[i].mergeMessageThreadNum) {
            threadPoolSize = _configVec[i].mergeMessageThreadNum;
        }
        if (_configVec[i].fbWriterRecycleSizeKb > 0) {
            FBMessageWriter::setRecycleBufferSize(_configVec[i].fbWriterRecycleSizeKb * 1024);
        }
        if (_configVec[i].tracingMsgLevel > 0) {
            traceFlag = true;
            if ((_configVec[i].tracingMsgLevel & 0x1) != 0) {
                if (!initTraceFileAlogger()) {
                    AUTIL_LOG(WARN, "init trace file alogger failed");
                }
                _tracingMsgInfoMap[_configVec[i].zkPath] = make_pair(true, SwiftWriterPtr());
            }

            if ((_configVec[i].tracingMsgLevel & 0x2) != 0) {
                auto tracingWriter = createTracingWriter(_configVec[i].zkPath);
                auto iter = _tracingMsgInfoMap.find(_configVec[i].zkPath);
                if (iter != _tracingMsgInfoMap.end()) {
                    iter->second.second = tracingWriter;
                } else {
                    _tracingMsgInfoMap[_configVec[i].zkPath] = make_pair(false, tracingWriter);
                }
            }
            if ((_configVec[i].tracingMsgLevel & 0x4) != 0) {
                auto iter = _tracingMsgInfoMap.find(_configVec[i].zkPath);
                auto *errResCollector = ResponseCollectorSingleton::getInstance();
                if (iter != _tracingMsgInfoMap.end() && iter->second.second != nullptr) {
                    errResCollector->setSwiftWriter(iter->second.second.get());
                } else {
                    auto traceWriter = createTracingWriter(_configVec[i].zkPath);
                    errResCollector->setSwiftWriter(traceWriter.get());
                    _tracingErrorResponseMap[_configVec[i].zkPath] = traceWriter;
                }
            }
        }
    }
    _mergeThreadPool.reset(new autil::ThreadPool(threadPoolSize, 128));
    if (!_mergeThreadPool->start("merge_msg")) {
        AUTIL_LOG(WARN, "start merge message thread failed.");
        return ERROR_UNKNOWN;
    }
    return ERROR_NONE;
}
SwiftWriterPtr SwiftClient::createTracingWriter(const std::string &zkPath) {
    SwiftWriterConfig tracingConfig;
    tracingConfig.zkPath = zkPath;
    tracingConfig.topicName = common::TRACING_MESSAGE_TOPIC_NAME;
    tracingConfig.functionChain = "hashId2partId";
    tracingConfig.compressThresholdInBytes = 1024;
    tracingConfig.maxBufferSize = 64 * 1024 * 1024;
    ErrorInfo errorInfo;
    SwiftWriterPtr tracingWriter(doCreateWriter({tracingConfig}, &errorInfo));
    if (errorInfo.errcode() != ERROR_NONE || tracingWriter == nullptr) {
        AUTIL_LOG(WARN, "create tracing message writer failed, topic name [%s]", common::TRACING_MESSAGE_TOPIC_NAME);
        return SwiftWriterPtr();
    } else {
        AUTIL_LOG(INFO, "create tracing message writer success, topic name [%s]", common::TRACING_MESSAGE_TOPIC_NAME);
        return tracingWriter;
    }
}

void SwiftClient::parseRetryTimesAndInterval() {
    for (size_t i = 0; i < _configVec.size(); i++) {
        _retryTimes = std::max(_configVec[i].retryTimes, _retryTimes);
        _retryTimeInterval = std::max(_configVec[i].retryTimeInterval, _retryTimeInterval);
    }
}

bool SwiftClient::initTraceFileAlogger() {
    string loggerFile = "logs/swift/trace.log";
    if (TraceFileLogger::initLogger(loggerFile)) {
        AUTIL_LOG(INFO, "add trace file logger success, path [%s].", loggerFile.c_str());
        return true;
    }
    return false;
    ;
}

SwiftReader *SwiftClient::createReader(const std::string &readerConfigStr, protocol::ErrorInfo *errorInfo) {
    if (readerConfigStr.empty()) {
        AUTIL_LOG(ERROR, "reader config empty");
        return NULL;
    }
    vector<SwiftReaderConfig> readerConfigVec;
    try {
        autil::legacy::FromJsonString(readerConfigVec, readerConfigStr);
    } catch (const autil::legacy::ExceptionBase &e) {}
    if (readerConfigVec.size() == 0) {
        const vector<string> &readerConfigStrVec =
            StringUtil::split(readerConfigStr, SwiftClientConfig::CLIENT_CONFIG_MULTI_SEPERATOR);
        for (size_t i = 0; i < readerConfigStrVec.size(); i++) {
            SwiftReaderConfig readerConfig;
            if (!readerConfig.parseFromString(readerConfigStrVec[i])) {
                if (errorInfo) {
                    errorInfo->set_errcode(ERROR_CLIENT_INVALID_PARAMETERS);
                }
                AUTIL_LOG(WARN, "parse reader config failed [%s]", readerConfigStrVec[i].c_str());
                return NULL;
            }
            readerConfigVec.push_back(readerConfig);
        }
    }
    if ((1 < readerConfigVec.size() && _clientZkPaths.size() != 0) ||
        (1 == readerConfigVec.size() && !adjustReaderConfigHack(readerConfigVec))) {
        if (errorInfo) {
            errorInfo->set_errcode(ERROR_CLIENT_INVALID_PARAMETERS);
        }
        return NULL;
    }
    if (1 == readerConfigVec.size()) {
        return doCreateReader(readerConfigVec[0], errorInfo);
    } else {
        return doCreateMultiReader(readerConfigVec, errorInfo);
    }
}

SwiftMultiReader *SwiftClient::doCreateMultiReader(const vector<SwiftReaderConfig> &readerConfigVec,
                                                   ErrorInfo *errorInfo) {
    SwiftMultiReader *multiReader = new SwiftMultiReader();
    for (const auto &readerConf : readerConfigVec) {
        if (readerConf.zkPath.empty()) {
            AUTIL_LOG(ERROR, "reader config zkPath empty, return");
            DELETE_AND_SET_NULL(multiReader);
            return NULL;
        }
        if (!readerConf.multiReaderOrder.empty() && !multiReader->setReaderOrder(readerConf.multiReaderOrder)) {
            AUTIL_LOG(ERROR, "multi reader order[%s] invalid", readerConf.multiReaderOrder.c_str());
            DELETE_AND_SET_NULL(multiReader);
            return NULL;
        }
        SwiftReader *reader = doCreateReader(readerConf, errorInfo, multiReader->getNotifier());
        if (reader && multiReader->addReader(reader)) {
            AUTIL_LOG(INFO, "create reader for topic[%s] sucess", readerConf.topicName.c_str());
        } else {
            string errorMsg("error");
            if (errorInfo) {
                errorMsg = errorInfo->errmsg();
            }
            AUTIL_LOG(ERROR,
                      "create reader for topic[%s] failed[%s], return",
                      readerConf.topicName.c_str(),
                      errorMsg.c_str());
            DELETE_AND_SET_NULL(multiReader);
            return NULL;
        }
    }
    AUTIL_LOG(INFO, "create multiReader sucess");
    return multiReader;
}

bool SwiftClient::adjustReaderConfigHack(vector<SwiftReaderConfig> &readerConfigVec) {
    if (_clientZkPaths.empty()) {
        return true;
    }
    const vector<string> &topicNames =
        StringUtil::split(readerConfigVec[0].topicName, CLIENT_CONFIG_MULTI_READER_SEPERATOR);
    if (topicNames.size() != _clientZkPaths.size()) {
        AUTIL_LOG(ERROR,
                  "topicName[%s] and client zkPath[%s] size not equal",
                  readerConfigVec[0].topicName.c_str(),
                  StringUtil::toString(_clientZkPaths).c_str());
        return false;
    }
    if (1 == topicNames.size()) {
        return true;
    }
    for (size_t idx = 1; idx < topicNames.size(); ++idx) {
        readerConfigVec[0].zkPath = _clientZkPaths[idx];
        readerConfigVec[0].topicName = topicNames[idx];
        readerConfigVec.push_back(readerConfigVec[0]);
    }
    readerConfigVec[0].zkPath = _clientZkPaths[0];
    readerConfigVec[0].topicName = topicNames[0];
    return true;
}

SwiftReader *SwiftClient::createReader(const string &topicName,
                                       const vector<uint32_t> &partitionId,
                                       const Filter &filter,
                                       const string &readerConfigStr,
                                       ErrorInfo *errorInfo) {
    SwiftReaderConfig config;
    if (!config.parseFromString(readerConfigStr)) {
        if (errorInfo) {
            errorInfo->set_errcode(ERROR_CLIENT_INVALID_PARAMETERS);
        }
        return NULL;
    }
    config.topicName = topicName;
    config.readPartVec = partitionId;
    config.swiftFilter = filter;
    return doCreateReader(config, errorInfo);
}

SwiftReader *SwiftClient::createReader(const string &topicName,
                                       const Filter &filter,
                                       const string &readerConfigStr,
                                       ErrorInfo *errorInfo) {
    SwiftReaderConfig config;
    if (!config.parseFromString(readerConfigStr)) {
        if (errorInfo) {
            errorInfo->set_errcode(ERROR_CLIENT_INVALID_PARAMETERS);
        }
        return NULL;
    }
    config.topicName = topicName;
    config.swiftFilter = filter;
    return doCreateReader(config, errorInfo);
}

SwiftReader *SwiftClient::doCreateReader(SwiftReaderConfig config, ErrorInfo *errorInfo, Notifier *notifier) {
    CREATE_WITH_FILTER_ERROR_CHECK(config.swiftFilter);
    SwiftAdminAdapterPtr adminAdapter = getAdminAdapter(config.zkPath);
    if (!adminAdapter) {
        if (errorInfo) {
            errorInfo->set_errcode(ERROR_CLIENT_NOT_INITED);
        }
        AUTIL_LOG(WARN, "Invalid zkPath [%s]", config.zkPath.c_str())
        return NULL;
    }
    if (config.zkPath.empty()) {
        config.zkPath = adminAdapter->getZkPath();
    }
    SwiftReader *reader = new SwiftReaderAdapter(adminAdapter, _channelManager, _metricsReporter.get());
    reader->setDecompressThreadPool(_mergeThreadPool);
    if (notifier) {
        SwiftReaderAdapter *readerAdapter = dynamic_cast<SwiftReaderAdapter *>(reader);
        readerAdapter->setNotifier(notifier);
    }
    RETRY_MODE_BEGIN;
    ec = reader->init(config);
    RETRY_MODE_END;

    CHECK_ERROR_CODE(reader);
    auto iter = _tracingMsgInfoMap.find(config.zkPath);
    if (iter != _tracingMsgInfoMap.end()) {
        SwiftReaderAdapter *readerAdapter = dynamic_cast<SwiftReaderAdapter *>(reader);
        vector<TraceLoggerPtr> loggerVec;
        if (iter->second.first) {
            TraceLoggerPtr fileLogger(new TraceFileLogger());
            loggerVec.push_back(fileLogger);
        }
        if (readerAdapter) {
            if (iter->second.second.get() != nullptr) {
                MessageMeta meta;
                meta.topicName = config.topicName;
                meta.mask =
                    autil::HashAlgorithm::hashString(config.topicName.c_str(), 1) % common::MAX_MSG_TRACING_MASK;
                meta.hashVal =
                    autil::HashAlgorithm::hashString(IpUtil::getIp().c_str(), 1) % common::MAX_MSG_TRACING_RANGE;
                TraceLoggerPtr swiftLogger(new TraceSwiftLogger(meta, iter->second.second.get()));
                loggerVec.push_back(swiftLogger);
            }
            ReadMessageTracerPtr msgTracer(new ReadMessageTracer(loggerVec));
            protocol::ReaderInfo readerInfo;
            readerInfo.set_topicname(config.topicName);
            *readerInfo.mutable_filter() = config.swiftFilter;
            readerInfo.set_readerid(config.readerIdentity);
            readerInfo.set_ip(IpUtil::getIp());
            readerInfo.set_pid(getpid());
            for (auto &clientConf : _configVec) {
                if (clientConf.zkPath == config.zkPath) {
                    readerInfo.set_clientid(clientConf.clientIdentity);
                    break;
                }
            }
            msgTracer->setReaderInfo(readerInfo);
            readerAdapter->setMessageTracer(msgTracer);
        }
    }
    return reader;
}

SwiftWriter *SwiftClient::createWriter(const std::string &writerConfigStr, protocol::ErrorInfo *errorInfo) {
    vector<SwiftWriterConfig> writerConfigVec;
    try {
        autil::legacy::FromJsonString(writerConfigVec, writerConfigStr);
    } catch (const autil::legacy::ExceptionBase &e) {}
    if (writerConfigVec.size() == 0) {
        const vector<string> &writerConfigStrVec =
            StringUtil::split(writerConfigStr, SwiftClientConfig::CLIENT_CONFIG_MULTI_SEPERATOR);
        for (size_t i = 0; i < writerConfigStrVec.size(); i++) {
            SwiftWriterConfig writerConfig;
            if (!writerConfig.parseFromString(writerConfigStrVec[i])) {
                if (errorInfo) {
                    errorInfo->set_errcode(ERROR_CLIENT_INVALID_PARAMETERS);
                }
                AUTIL_LOG(WARN, "parse write config failed [%s]", writerConfigStrVec[i].c_str());
                return NULL;
            }
            writerConfigVec.push_back(writerConfig);
        }
    }
    return doCreateWriter(writerConfigVec, errorInfo);
}

SwiftWriter *SwiftClient::doCreateWriter(const vector<SwiftWriterConfig> &configVec, ErrorInfo *errorInfo) {
    if (1 != configVec.size()) {
        AUTIL_LOG(ERROR, "writer only support one config, current size[%ld]", configVec.size());
        if (errorInfo) {
            errorInfo->set_errcode(ERROR_CLIENT_INVALID_PARAMETERS);
        }
        return NULL;
    }

    SwiftWriterConfig config = configVec[0];
    if (!config.isValidate()) {
        if (errorInfo) {
            errorInfo->set_errcode(ERROR_CLIENT_INVALID_PARAMETERS);
        }
        return NULL;
    }
    if (_adminAdapterMap.size() > 1) {
        if (config.zkPath.empty()) {
            if (errorInfo) {
                errorInfo->set_errcode(ERROR_CLIENT_INVALID_PARAMETERS);
            }
            return NULL;
        }
    }
    SwiftAdminAdapterPtr adminAdapter = getAdminAdapter(config.zkPath);
    if (!adminAdapter) {
        if (errorInfo) {
            errorInfo->set_errcode(ERROR_CLIENT_INVALID_PARAMETERS);
        }
        return NULL;
    }

    if (config.zkPath.empty()) {
        config.zkPath = adminAdapter->getZkPath();
    }
    BlockPoolPtr blockPool = getBlockPool(config.zkPath);
    BufferSizeLimiterPtr limiter = getLimiter(config.zkPath);
    SwiftWriterAdapter *writer = new SwiftWriterAdapter(
        adminAdapter, _channelManager, _mergeThreadPool, limiter, blockPool, _metricsReporter.get());
    if (writer == NULL) {
        return NULL;
    }
    RETRY_MODE_BEGIN;
    ec = writer->init(config);
    RETRY_MODE_END;

    CHECK_ERROR_CODE(writer);
    return writer;
}

SwiftWriter *SwiftClient::createWriter(const string &topicName, const string &writerConfigStr, ErrorInfo *errorInfo) {
    SwiftWriterConfig config;
    if (!config.parseFromString(writerConfigStr)) {
        if (errorInfo) {
            errorInfo->set_errcode(ERROR_CLIENT_INVALID_PARAMETERS);
        }
        AUTIL_LOG(WARN, "parse write config failed [%s]", writerConfigStr.c_str());
        return NULL;
    }
    config.topicName = topicName;
    vector<SwiftWriterConfig> writerConfigVec;
    writerConfigVec.push_back(config);
    return doCreateWriter(writerConfigVec, errorInfo);
}

bool SwiftClient::initAlog(const string &content) {
    static autil::ThreadMutex mutex;
    static bool isInited = false;
    autil::ScopedLock lock(mutex);
    if (isInited) {
        return true;
    }
    try {
        alog::Configurator::configureLoggerFromString(content.c_str());
    } catch (std::exception &e) {
        std::cerr << "configure logger use [" << content << "] failed" << std::endl;
        AUTIL_ROOT_LOG_CONFIG();
        return false;
    }
    isInited = true;
    return true;
}

SwiftAdminAdapterPtr SwiftClient::getAdminAdapter() {
    if (_adminAdapterMap.empty()) {
        return SwiftAdminAdapterPtr();
    }
    return _adminAdapterMap.begin()->second;
}

SwiftAdminAdapterPtr SwiftClient::getAdminAdapter(const std::string &zkPath) {
    if (zkPath.empty() && _adminAdapterMap.size() == 1) {
        return _adminAdapterMap.begin()->second;
    }

    map<string, SwiftAdminAdapterPtr>::iterator iter = _adminAdapterMap.find(zkPath);
    if (iter == _adminAdapterMap.end()) {
        return SwiftAdminAdapterPtr();
    }
    return iter->second;
}

BlockPoolPtr SwiftClient::getBlockPool(const std::string &zkPath) {
    if (zkPath.empty() && _blockPoolMap.size() == 1) {
        return _blockPoolMap.begin()->second;
    }
    map<string, BlockPoolPtr>::iterator iter = _blockPoolMap.find(zkPath);
    if (iter == _blockPoolMap.end()) {
        return BlockPoolPtr();
    }
    return iter->second;
}

BufferSizeLimiterPtr SwiftClient::getLimiter(const std::string &zkPath) {
    if (zkPath.empty() && _limiterMap.size() == 1) {
        return _limiterMap.begin()->second;
    }
    map<string, BufferSizeLimiterPtr>::iterator iter = _limiterMap.find(zkPath);
    if (iter == _limiterMap.end()) {
        return BufferSizeLimiterPtr();
    }
    return iter->second;
}

SwiftAdminAdapterPtr SwiftClient::createSwiftAdminAdapter(const SwiftClientConfig &config,
                                                          SwiftRpcChannelManagerPtr channelManager) {
    SwiftAdminAdapterPtr adapter(new SwiftAdminAdapter(
        config.zkPath, channelManager, config.useFollowerAdmin, config.adminTimeout, config.refreshTime));
    adapter->setLatelyErrorTimeMinIntervalUs(config.latelyErrorTimeMinIntervalUs);
    adapter->setLatelyErrorTimeMaxIntervalUs(config.latelyErrorTimeMaxIntervalUs);
    ClientAuthorizerInfo info;
    info.username = config.username;
    info.passwd = config.passwd;
    adapter->setAuthenticationConf(info);
    return adapter;
}

size_t SwiftClient::getPhyMemSize() {
    struct sysinfo s_info;
    sysinfo(&s_info);
    return s_info.totalram;
}

} // namespace client
} // namespace swift
