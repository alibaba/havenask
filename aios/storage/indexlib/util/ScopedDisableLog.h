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

#include "alog/Logger.h"

namespace indexlib { namespace util {

class ScopedDisableLog
{
public:
    ScopedDisableLog(const char* loggerName) : _loggerName(loggerName)
    {
        _originalLevel = alog::Logger::getLogger(loggerName)->getLevel();
        alog::Logger::getLogger(loggerName)->setLevel(alog::LOG_LEVEL_FATAL);
    }
    ~ScopedDisableLog() { alog::Logger::getLogger(_loggerName)->setLevel(_originalLevel); }

private:
    const char* _loggerName = nullptr;
    uint32_t _originalLevel = alog::LOG_LEVEL_INFO;
};
}} // namespace indexlib::util
