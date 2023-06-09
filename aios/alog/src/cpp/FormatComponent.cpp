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
#include "FormatComponent.h"

#include "LoggingEvent.h"

namespace alog
{
StringComponent::StringComponent(const std::string& literal) : m_literal(literal) {}

void StringComponent::append(std::ostringstream& out, const LoggingEvent& event)
{
    out << m_literal;
}

void MessageComponent::append(std::ostringstream& out, const LoggingEvent& event)
{
    out << event.message;
}

void NameComponent::append(std::ostringstream& out, const LoggingEvent& event)
{
    out << event.loggerName;
}

void DateComponent::append(std::ostringstream& out, const LoggingEvent& event)
{
    out << event.loggingTime;
}

void ZoneDateComponent::append(std::ostringstream& out, const LoggingEvent& event)
{
    out << event.loggingZoneTime;
}

void LevelComponent::append(std::ostringstream& out, const LoggingEvent& event)
{
    out << event.levelStr;
}

void ProcessIdComponent::append(std::ostringstream& out, const LoggingEvent& event)
{
    out << event.pid;
}

void ThreadIdComponent::append(std::ostringstream& out, const LoggingEvent& event)
{
    out << event.tid;
}

void FileComponent::append(std::ostringstream& out, const LoggingEvent& event)
{
    out << event.file;
}

void LineComponent::append(std::ostringstream& out, const LoggingEvent& event)
{
    out << event.line;
}

void FunctionComponent::append(std::ostringstream& out, const LoggingEvent& event)
{
    out << event.function;
}

}
