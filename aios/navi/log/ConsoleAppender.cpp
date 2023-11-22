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
#include "navi/log/ConsoleAppender.h"
#include "navi/log/NaviLogger.h"
#include <iostream>

namespace navi {

ConsoleAppender::ConsoleAppender(const std::string &levelStr)
    : Appender(levelStr, nullptr)
{
    auto patternLayout = std::make_shared<PatternLayout>();
    patternLayout->setLogPattern(DEFAULT_LOG_PATTERN);
    setLayout(patternLayout);
}

ConsoleAppender::~ConsoleAppender() {
}

int ConsoleAppender::append(LoggingEvent& event) {
    std::string formatedStr = m_layout->format(event);
    if (formatedStr.length() > 0) {
        autil::ScopedLock lock(_lock);
        std::cout << formatedStr << std::flush;
    }
    return formatedStr.length();
}

}

