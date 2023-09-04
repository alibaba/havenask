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
#ifndef NAVI_LOGCONFIG_H
#define NAVI_LOGCONFIG_H

#include "navi/common.h"
#include "autil/legacy/jsonizable.h"
#include "navi/log/LogBtFilter.h"

namespace navi {

class LogConfig;

class FileLogConfig : public autil::legacy::Jsonizable
{
public:
    FileLogConfig();
    ~FileLogConfig();
private:
    FileLogConfig(const LogConfig &);
    FileLogConfig &operator=(const LogConfig &);
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
public:
    uint32_t translateDelayTime() const;
private:
    static std::string transformPattern(const std::string &inputPattern);
    static std::string getProgressName();
    static std::string getLastDirectory(uint32_t layers);
public:
    std::string fileName;
    std::string levelStr;
    std::string logPattern;
    bool autoFlush;
    size_t maxFileSize;
    std::string delayTimeStr;
    bool compress;
    size_t cacheLimit;
    size_t logKeepCount;
    bool asyncFlush;
    size_t flushThreshold;
    size_t flushInterval;
    std::vector<LogBtFilterParam> btFilters;
private:
    static const size_t DEFAULT_FLUSH_THRESHOLD_IN_KB = 1024;
    static const size_t DEFAULT_FLUSH_INTERVAL_IN_MS = 1000;
};

class LogConfig : public autil::legacy::Jsonizable
{
public:
    LogConfig()
        : maxMessageLength(DEFAULT_MAX_MESSAGE_LENGTH)
    {
    }
    ~LogConfig() {
    }
private:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
public:
    static LogConfig getDefaultLogConfig();
public:
    size_t maxMessageLength;
    std::vector<FileLogConfig> fileAppenders;
private:
    static const size_t DEFAULT_MAX_MESSAGE_LENGTH = 1024 * 1024;
};

}

#endif //NAVI_LOGCONFIG_H
