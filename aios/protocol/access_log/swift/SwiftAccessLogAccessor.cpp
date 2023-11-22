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
#include "access_log/swift/SwiftAccessLogAccessor.h"

#include "autil/Log.h"
#include "autil/EnvUtil.h"
#include "swift/client/SwiftClient.h"

using namespace std;
using namespace swift::client;
using namespace swift::protocol;
using namespace autil::legacy;

AUTIL_DECLARE_AND_SETUP_LOGGER(access_log, SwiftAccessLogAccessor);

namespace access_log {

const string SwiftAccessLogAccessor::DEFAULT_SITE = "ea120";

SwiftAccessLogAccessor::SwiftAccessLogAccessor(const std::string &logName,
                                               const autil::legacy::Any &config,
                                               const autil::PartitionRange &partRange)
    : AccessLogAccessor(logName), _config(config), _partRange(partRange), _swiftClient(new SwiftClient()) {}

SwiftAccessLogAccessor::~SwiftAccessLogAccessor() {
    if (_swiftClient) {
        delete _swiftClient;
    }
}

bool SwiftAccessLogAccessor::init() {
    map<string, SwiftAccessLogConfig> configMap;
    try {
        FromJson(configMap, _config);
    } catch (const autil::legacy::ExceptionBase &e) {
        AUTIL_LOG(ERROR, "parse from config str failed");
        return false;
    }

    string siteInfo = autil::EnvUtil::getEnv("siteInfo", DEFAULT_SITE);
    if (configMap.empty()) {
        AUTIL_LOG(ERROR, "SwiftAccessLogAccessor parse config failed");
        return false;
    } else if (configMap.find(siteInfo) == configMap.end()) {
        AUTIL_LOG(WARN, "site swift config for %s not found, use default config", siteInfo.c_str());
        _swiftConfig = configMap.begin()->second;
    } else {
        _swiftConfig = configMap[siteInfo];
    }

    _swiftConfig._writerConfig.topicName = _logName;
    _swiftConfig._writerConfig.functionChain = "hashId2partId";

    _swiftConfig._readerConfig.topicName = _logName;
    _swiftConfig._readerConfig.swiftFilter.set_from(_partRange.first);
    _swiftConfig._readerConfig.swiftFilter.set_to(_partRange.second);

    auto ec = _swiftClient->init(_swiftConfig._clientConfig);
    if (ec != swift::protocol::ERROR_NONE) {
        AUTIL_LOG(ERROR, "init swift client failed, error code[%d]", int(ec));
        return false;
    }
    return true;
}

AccessLogWriter *SwiftAccessLogAccessor::createWriter() {
    SwiftWriter *swiftWriter = createSwiftWriter();
    if (!swiftWriter) {
        return nullptr;
    }

    SwiftAccessLogWriter *writer = new SwiftAccessLogWriter(_logName, _partRange, swiftWriter);
    writer->init();
    return writer;
}

AccessLogReader *SwiftAccessLogAccessor::createReader() {
    SwiftReader *swiftReader = createSwiftReader();
    if (!swiftReader) {
        return nullptr;
    }

    SwiftAccessLogReader *reader = new SwiftAccessLogReader(_logName, swiftReader);
    if (!reader->init()) {
        delete reader;
        return nullptr;
    }
    return reader;
}

SwiftWriter *SwiftAccessLogAccessor::createSwiftWriter() {
    ErrorInfo errorInfo;
    string writerConfigStr = ToJsonString(_swiftConfig._writerConfig);
    SwiftWriter *swiftWriter = _swiftClient->createWriter(writerConfigStr, &errorInfo);

    if (errorInfo.has_errcode()) {
        if (swiftWriter) {
            delete swiftWriter;
        }
        AUTIL_LOG(ERROR, "create swift writer failed, errorCode:%d", errorInfo.errcode());
        return nullptr;
    }

    return swiftWriter;
}

SwiftReader *SwiftAccessLogAccessor::createSwiftReader() {
    ErrorInfo errorInfo;
    string readerConfigStr = ToJsonString(_swiftConfig._readerConfig);

    SwiftReader *swiftReader = _swiftClient->createReader(readerConfigStr, &errorInfo);

    if (errorInfo.has_errcode()) {
        if (swiftReader) {
            delete swiftReader;
        }
        AUTIL_LOG(ERROR, "create swift reader failed, errorCode:%d", errorInfo.errcode());
        return nullptr;
    }

    return swiftReader;
}

} // namespace access_log
