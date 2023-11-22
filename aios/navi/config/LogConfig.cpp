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
#include "navi/config/LogConfig.h"
#include <fstream>
#include <sys/types.h>
#include <unistd.h>
#include <linux/limits.h>

namespace navi {

FileLogConfig::FileLogConfig()
    : fileName(DEFAULT_LOG_FILE_NAME)
    , levelStr("ERROR")
    , logPattern(DEFAULT_LOG_PATTERN)
    , autoFlush(false)
    , maxFileSize(DEFAULT_MAX_FILE_SIZE_IN_MB)
    , compress(true)
    , cacheLimit(DEFAULT_CACHE_LIMIT_IN_MB)
    , logKeepCount(DEFAULT_LOG_KEEP_COUNT)
    , asyncFlush(true)
    , flushThreshold(DEFAULT_FLUSH_THRESHOLD_IN_KB)
    , flushInterval(DEFAULT_FLUSH_INTERVAL_IN_MS)
{
}

FileLogConfig::~FileLogConfig() {
}

void FileLogConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("file_name", fileName, fileName);
    json.Jsonize("log_level", levelStr, levelStr);
    json.Jsonize("log_pattern", logPattern, logPattern);
    json.Jsonize("flush", autoFlush, autoFlush);
    json.Jsonize("max_file_size", maxFileSize, maxFileSize);
    json.Jsonize("delay_time", delayTimeStr, delayTimeStr);
    json.Jsonize("compress", compress, compress);
    json.Jsonize("cache_limit", cacheLimit, cacheLimit);
    json.Jsonize("log_keep_count", logKeepCount, logKeepCount);
    json.Jsonize("async_flush", asyncFlush, asyncFlush);
    json.Jsonize("flush_threshold", flushThreshold, flushThreshold);
    json.Jsonize("flush_interval", flushInterval, flushInterval);
    json.Jsonize("bt_filters", btFilters, btFilters);
    if (json.GetMode() == FROM_JSON) {
        fileName = transformPattern(fileName);
        if (0 == flushThreshold) {
            flushThreshold = DEFAULT_FLUSH_THRESHOLD_IN_KB;
        }
        if (0 == flushInterval) {
            flushInterval = DEFAULT_FLUSH_INTERVAL_IN_MS;
        }
        if (autoFlush) {
            asyncFlush = false;
        }
    }
}

std::string FileLogConfig::transformPattern(const std::string &inputPattern) {
    std::string result;
    result.reserve(inputPattern.length());
    for (size_t i = 0; i < inputPattern.length(); ++i) {
        if (inputPattern[i] != '%' || i + 1 >= inputPattern.length()) {
            result.append(1, inputPattern[i]);
            continue;
        }
        ++i;
        switch (inputPattern[i]) {
        case '%':
            result.append(1, '%');
            break;
        case 'e':
            result.append(getProgressName());
            break;
        case 'p':
            char buf[10];
            sprintf(buf, "%d", getpid());
            result.append(buf);
            break;
        case 'c':
            result.append(getLastDirectory(1));
            break;
        case 'C':
            result.append(getLastDirectory(2));
            break;
        default:
            result.append(1, '%');
            result.append(1, inputPattern[i]);
            break;
        }
    }
    return result;
}

std::string FileLogConfig::getProgressName() {
    char buffer[64];
    sprintf(buffer, "/proc/%d/cmdline", getpid());
    std::ifstream fin(buffer);
    std::string line;
    if (!getline(fin, line)) {
        throw std::runtime_error(std::string("get line failed: filename[") + buffer + "]");
    }
    std::string procName = line.substr(0, line.find('\0'));
    size_t start = procName.rfind('/') + 1;
    size_t end = procName.length() - 1;
    return procName.substr(start, end - start + 1);
}

std::string FileLogConfig::getLastDirectory(uint32_t layers) {
    char cwdPath[PATH_MAX];
    char* ret = getcwd(cwdPath, PATH_MAX);
    if (NULL == ret) {
        throw std::runtime_error(std::string("getcwd failed."));
    }
    std::string path = std::string(cwdPath);
    if ('/' == *(path.rbegin())) {
        path.resize(path.length() - 1);
    }
    size_t start = path.length() - 1;
    for (uint32_t i = 0; i < layers; ++i) {
        start = path.rfind('/', start - 1);
        if (start == 0) {
            break;
        }
    }
    return start == 0 ? path : path.substr(start + 1);
}

uint32_t FileLogConfig::translateDelayTime() const {
    auto str = delayTimeStr;
    int delay = 0;
    if (str == std::string())
    {
        return 0;
    }
    switch (*str.rbegin())
    {
    case 's':
    case 'S':
        delay = atoi(str.c_str());
        break;
    case 'm':
    case 'M':
        delay = atoi(str.c_str()) * 60;
        break;
    case 'd':
    case 'D':
        delay = atoi(str.c_str()) * 24 * 3600;
        break;
    case 'h':
    case 'H':
    default:
        delay = atoi(str.c_str()) * 3600;
    }
    if (delay < 0)
    {
        delay = 0;
    }
    return delay;
}

void LogConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("max_message_length", maxMessageLength, maxMessageLength);
    json.Jsonize("file_appenders", fileAppenders, fileAppenders);
}

LogConfig LogConfig::getDefaultLogConfig() {
    LogConfig logConfig;
    FileLogConfig fileConfig;
    fileConfig.fileName = "./logs/navi_default.log";
    fileConfig.levelStr = "INFO";
    fileConfig.asyncFlush = false;
    logConfig.fileAppenders.emplace_back(std::move(fileConfig));
    return logConfig;
}

}
