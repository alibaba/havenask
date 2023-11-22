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
#ifndef ISEARCH_ACCESS_LOG_ACCESSLOG_H
#define ISEARCH_ACCESS_LOG_ACCESSLOG_H

#include <functional>

#include "access_log/AccessLogAccessorFactory.h"
#include "access_log/AccessLogReader.h"
#include "access_log/AccessLogWriter.h"
#include "access_log/PbAccessLog.pb.h"
#include "access_log/common.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"

namespace access_log {

class AccessLog {
public:
    AccessLog(const std::string &logName,
              const std::string &configStr,
              const autil::PartitionRange partRange = {0, autil::RangeUtil::MAX_PARTITION_RANGE},
              uint32_t sampleStep = DEFAULT_INVALID_VALUE,
              uint64_t sampleInterval = DEFAULT_INVALID_VALUE);
    AccessLog(const std::string &logName,
              const autil::legacy::json::JsonMap &configMap,
              const autil::PartitionRange partRange = {0, autil::RangeUtil::MAX_PARTITION_RANGE},
              uint32_t sampleStep = DEFAULT_INVALID_VALUE,
              uint64_t sampleInterval = DEFAULT_INVALID_VALUE);
    ~AccessLog();

private:
    AccessLog(const AccessLog &) = delete;
    AccessLog &operator=(const AccessLog &) = delete;

public:
    bool init(bool needLocalAccessor = true);

    void write(const PbAccessLog &pbAccessLog);

    bool read(uint32_t count, std::vector<PbAccessLog> &pbAccessLog, int32_t statusCode = INT_MIN);

    // write 自定义log
    void writeCustomLog(const std::string &customLog);
    void writeCustomLog(const std::function<std::string()> &genLogFunc);

    // read 自定义log, 支持不采样 TODO(luoli.hn) 等空了再来实现
    bool readCustomLog(uint32_t count, std::vector<std::string> &customLogs);

    // do release after read if it takes long time util next read
    void releaseReaderResource();

private:
    void create(const std::string &type, const autil::legacy::Any &config);

    AccessLogAccessor *getAccessor(const std::string &key, const autil::legacy::Any &config);

    bool doSample();

    void initParam();

private:
    std::string _logName;
    std::string _configStr;
    autil::legacy::json::JsonMap _configMap;
    autil::PartitionRange _partRange;
    std::map<std::string, AccessLogAccessor *> _accessorMap;
    std::vector<AccessLogWriter *> _writers;
    std::vector<AccessLogReader *> _readers;
    std::atomic<uint64_t> _lastLogTimestamp;
    uint32_t _sampleStep;
    uint64_t _sampleInterval;

public:
    static const uint32_t DEFAULT_INVALID_VALUE = -1;
    static const uint32_t DEFAULT_SAMPLE_STEP = 10;
    static const uint64_t DEFAULT_SAMPLE_INTERVAL = 5 * 60; // 5min
};

struct AccessLogOptions : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;

public:
    uint32_t _sampleStep = AccessLog::DEFAULT_INVALID_VALUE;
    uint32_t _sampleInterval = AccessLog::DEFAULT_INVALID_VALUE;
};

} // namespace access_log

#endif // ISEARCH_ACCESS_LOG_ACCESSLOG_H
