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
#include <stdint.h>
#include <string>

#include "autil/Log.h"

#define ERROR_COLLECTOR_LOG(level, format, args...)                                                                    \
    if (indexlib::util::ErrorLogCollector::UsingErrorLogCollect()) {                                                   \
        do {                                                                                                           \
            thread_local int64_t logCounter = 0;                                                                       \
            logCounter++;                                                                                              \
            indexlib::util::ErrorLogCollector::IncTotalErrorCount();                                                   \
            if (logCounter == (logCounter & -logCounter)) {                                                            \
                ALOG_LOG(indexlib::util::ErrorLogCollector::GetLogger(), alog::LOG_LEVEL_##level,                      \
                         "%s, error_count[%ld], error_msg: " format,                                                   \
                         indexlib::util::ErrorLogCollector::GetIdentifier().c_str(), logCounter, ##args);              \
            }                                                                                                          \
        } while (0);                                                                                                   \
    }

#define DECLARE_ERROR_COLLECTOR_IDENTIFIER(identifier)                                                                 \
    indexlib::util::ErrorLogCollector::SetIdentifier(identifier);                                                      \
    string envStr = autil::EnvUtil::getEnv("COLLECT_ERROR_LOG");                                                       \
    if (envStr.empty() || envStr != "false") {                                                                         \
        indexlib::util::ErrorLogCollector::EnableErrorLogCollect();                                                    \
    }

namespace indexlib { namespace util {

class ErrorLogCollector
{
public:
    // generation, role name, errorCount, errorMsg
    static void SetIdentifier(const std::string& identifier) { _identifier = identifier; }
    static alog::Logger* GetLogger() { return _logger; }
    static std::string& GetIdentifier() { return _identifier; }
    static bool UsingErrorLogCollect() { return _useErrorLogCollector; }
    static void EnableErrorLogCollect() { _useErrorLogCollector = true; }

    static int64_t GetTotalErrorCount() { return _totalErrorCount; }
    static void IncTotalErrorCount()
    {
        if (_enableTotalErrorCount) {
            _totalErrorCount += 1;
        }
    }
    static void ResetTotalErrorCount() { _totalErrorCount = 0; }
    static void EnableTotalErrorCount() { _enableTotalErrorCount = true; }

private:
    static alog::Logger* _logger;
    static std::string _identifier;
    static bool _useErrorLogCollector;
    static bool _enableTotalErrorCount;
    static std::atomic_long _totalErrorCount;
};
}} // namespace indexlib::util
