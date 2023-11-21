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
#include "access_log/AccessLog.h"

#include <stdlib.h>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/EnvUtil.h"
#include "autil/legacy/jsonizable.h"

using namespace std;

AUTIL_DECLARE_AND_SETUP_LOGGER(access_log, AccessLog);

namespace access_log {

AccessLog::AccessLog(const string &logName,
                     const string &configStr,
                     const autil::PartitionRange partRange,
                     uint32_t sampleStep,
                     uint64_t sampleInterval)
    : _logName(logName)
    , _configStr(configStr)
    , _partRange(partRange)
    , _sampleStep(sampleStep)
    , _sampleInterval(sampleInterval) {
    _lastLogTimestamp = 0;
    initParam();
}

AccessLog::AccessLog(const string &logName,
                     const autil::legacy::json::JsonMap &configMap,
                     const autil::PartitionRange partRange,
                     uint32_t sampleStep,
                     uint64_t sampleInterval)
    : _logName(logName)
    , _configMap(configMap)
    , _partRange(partRange)
    , _sampleStep(sampleStep)
    , _sampleInterval(sampleInterval) {
    _lastLogTimestamp = 0;
    initParam();
}

AccessLog::~AccessLog() {
    for (auto writer : _writers) {
        if (writer) {
            delete writer;
        }
    }
    for (auto reader : _readers) {
        if (reader) {
            delete reader;
        }
    }
    for (auto it = _accessorMap.begin(); it != _accessorMap.end(); it++) {
        if (it->second) {
            delete it->second;
        }
    }
}

void AccessLog::initParam() {
    if (!_configStr.empty()) {
        try {
            autil::legacy::FromJsonString(_configMap, _configStr);
        } catch (const autil::legacy::ExceptionBase &e) {
            AUTIL_LOG(ERROR, "parse config string failed, error info[%s]", e.ToString().c_str());
        }
        return;
    }

    if (_configMap.find("access_log_options") != _configMap.end()) {
        AccessLogOptions accessLogOptions;
        FromJson(accessLogOptions, _configMap["access_log_options"]);
        _sampleStep = accessLogOptions._sampleStep;
        _sampleInterval = accessLogOptions._sampleInterval;
        _configMap.erase("access_log_options");
    }

    if (_sampleStep == DEFAULT_INVALID_VALUE) {
        int32_t tmpCount = 0;
        if (autil::EnvUtil::getEnvWithoutDefault("accessLogSampleCount", tmpCount) && tmpCount >= 0) {
            _sampleStep = tmpCount;
        } else {
            _sampleStep = DEFAULT_SAMPLE_STEP;
        }
    }

    if (_sampleInterval == DEFAULT_INVALID_VALUE) {
        uint64_t tempInterval = 0;
        if (autil::EnvUtil::getEnvWithoutDefault("accessLogSampleInterval", tempInterval) && tempInterval > 0) {
            _sampleInterval = tempInterval;
        } else {
            _sampleInterval = DEFAULT_SAMPLE_INTERVAL;
        }
    }
    AUTIL_LOG(INFO, "access log sample count value:[%u]", _sampleStep);
    AUTIL_LOG(INFO, "access log sample interval value:[%lu]", _sampleInterval);
}

bool AccessLog::init(bool needLocalAccessor) {
    // init accessor by config
    for (auto it = _configMap.begin(); it != _configMap.end(); it++) {
        create(it->first, it->second);
    }
    if (needLocalAccessor) {
        create(LOCAL_TYPE, autil::legacy::Any());
    }
    if (_writers.empty()) {
        AUTIL_LOG(WARN, "accessor is empty");
        return false;
    }
    return true;
}

void AccessLog::create(const std::string &type, const autil::legacy::Any &config) {
    auto accessor = getAccessor(type, config);
    if (!accessor) {
        return;
    }
    AccessLogWriter *writer = accessor->createWriter();
    if (!writer) {
        return;
    }
    AccessLogReader *reader = accessor->createReader();
    if (!reader) {
        delete writer;
        return;
    }
    _writers.push_back(writer);
    _readers.push_back(reader);
}

void AccessLog::write(const PbAccessLog &pbAccessLog) {
    if (!doSample()) {
        return;
    }
    string pbAccessLogStr;
    if (!pbAccessLog.SerializeToString(&pbAccessLogStr)) {
        AUTIL_LOG(ERROR, "serialize pbAccessLog failed.");
        return;
    }

    for (const auto writer : _writers) {
        writer->write(pbAccessLogStr);
    }
}

bool AccessLog::read(uint32_t count, vector<PbAccessLog> &pbAccessLog, int32_t statusCode) {
    for (const auto reader : _readers) {
        if (!reader->isActive()) {
            if (!reader->recover()) {
                AUTIL_LOG(
                    ERROR, "recover %s writer for logName %s failed.", reader->getType().c_str(), _logName.c_str());
            }
        }
        if (reader->read(count, pbAccessLog, statusCode) && !pbAccessLog.empty()) {
            return true;
        } else {
            AUTIL_LOG(WARN, "%s read log failed, try other way", reader->getType().c_str());
        }
    }

    AUTIL_LOG(ERROR, "read access_log all failed.");
    return false;
}

// write 自定义log
void AccessLog::writeCustomLog(const std::string &customLog) {
    if (!doSample()) {
        return;
    }
    for (const auto writer : _writers) {
        writer->write(customLog);
    }
}

void AccessLog::writeCustomLog(const std::function<std::string()> &genLogFunc) {
    if (!doSample()) {
        return;
    }
    string customLog = genLogFunc();
    if (customLog.empty()) {
        return;
    }
    for (const auto writer : _writers) {
        writer->write(customLog);
    }
}

// read 自定义log
bool AccessLog::readCustomLog(uint32_t count, std::vector<std::string> &customLogs) {
    for (const auto reader : _readers) {
        if (!reader->isActive()) {
            if (!reader->recover()) {
                AUTIL_LOG(
                    ERROR, "recover %s writer for logName %s failed.", reader->getType().c_str(), _logName.c_str());
            }
        }
        if (reader->readCustomLog(count, customLogs) && !customLogs.empty()) {
            return true;
        } else {
            AUTIL_LOG(WARN, "%s read log failed, try other way", reader->getType().c_str());
        }
    }

    AUTIL_LOG(ERROR, "read custom_log all failed.");
    return false;
}

void AccessLog::releaseReaderResource() {
    for (auto &reader : _readers) {
        reader->releaseResource();
    }
}

AccessLogAccessor *AccessLog::getAccessor(const string &key, const autil::legacy::Any &config) {
    auto it = _accessorMap.find(key);
    if (it != _accessorMap.end()) {
        return it->second;
    }
    AccessLogAccessor *accessor = AccessLogAccessorFactory::getAccessor(_logName, key, config, _partRange);
    if (!accessor) {
        return nullptr;
    }
    if (!accessor->init()) {
        delete accessor;
        AUTIL_LOG(INFO, "init failed");
        return nullptr;
    }

    _accessorMap[key] = accessor;
    return accessor;
}

bool AccessLog::doSample() {
    // _sampleStep 用于控制是否采样和每条必采，其他看时间间隔
    if (_sampleStep == 0) {
        return false;
    } else if (_sampleStep == 1) {
        return true;
    }
    uint64_t currentTimestamp = autil::TimeUtility::currentTimeInSeconds();
    uint64_t lastTimestamp = _lastLogTimestamp.load(std::memory_order_relaxed);
    if (lastTimestamp + _sampleInterval > currentTimestamp) {
        return false;
    }
    _lastLogTimestamp.store(currentTimestamp, std::memory_order_relaxed);
    return true;
}

void AccessLogOptions::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("sample_step", _sampleStep, _sampleStep);
    json.Jsonize("sample_interval", _sampleInterval, _sampleInterval);
}

} // namespace access_log
