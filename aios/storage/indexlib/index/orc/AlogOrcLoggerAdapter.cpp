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
#include "indexlib/index/orc/AlogOrcLoggerAdapter.h"

#include <assert.h>
#include <stdarg.h>

namespace indexlibv2::index {

AlogOrcLoggerAdapter::AlogOrcLoggerAdapter(const std::string& path)
{
    _logger = alog::Logger::getLogger(path.c_str());
    assert(_logger != nullptr);
}

void AlogOrcLoggerAdapter::logV(OrcLogLevel level, const char* fname, int lineno, const char* function, const char* fmt,
                                ...)
{
    auto alogLevel = ConvertOrcLevelToAlogLevel(level);
    va_list args;
    va_start(args, fmt);
    _logger->log(alogLevel, fname, lineno, function, fmt, args);
    va_end(args);
}

void AlogOrcLoggerAdapter::log(OrcLogLevel level, const char* fname, int lineno, const char* function,
                               const std::string& key, const std::string& value)
{
    auto alogLevel = ConvertOrcLevelToAlogLevel(level);
    _logger->log(alogLevel, fname, lineno, function, "%s: %s", key.c_str(), value.c_str());
}

bool AlogOrcLoggerAdapter::isLevelEnabled(OrcLogLevel level) const
{
    auto alogLevel = ConvertOrcLevelToAlogLevel(level);
    return _logger->isLevelEnabled(alogLevel);
}

uint32_t AlogOrcLoggerAdapter::ConvertOrcLevelToAlogLevel(OrcLogLevel level)
{
    switch (level) {
    case ORC_LOG_LEVEL_DEBUG:
        return alog::LOG_LEVEL_DEBUG;
    case ORC_LOG_LEVEL_INFO:
        return alog::LOG_LEVEL_INFO;
    case ORC_LOG_LEVEL_ERROR:
        return alog::LOG_LEVEL_ERROR;
    default:
        return alog::LOG_LEVEL_DISABLE;
    }
}

} // namespace indexlibv2::index
