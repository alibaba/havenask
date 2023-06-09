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
#ifndef _ALOG_FORMAT_COMPONENT_H_
#define _ALOG_FORMAT_COMPONENT_H_

#include <sstream>
#include <string>

namespace alog {
struct LoggingEvent;

class FormatComponent 
{
public:
    inline virtual ~FormatComponent() {};
    virtual void append(std::ostringstream& out, const LoggingEvent& event) = 0;
};

class StringComponent : public FormatComponent 
{
public:
    StringComponent(const std::string& literal);

    virtual void append(std::ostringstream& out, const LoggingEvent& event);
private:
    std::string m_literal;
};

class MessageComponent : public FormatComponent 
{
public:
    virtual void append(std::ostringstream& out, const LoggingEvent& event);
};


class NameComponent : public FormatComponent 
{
public:
    virtual void append(std::ostringstream& out, const LoggingEvent& event);
};

class DateComponent : public FormatComponent 
{
public:
    virtual void append(std::ostringstream& out, const LoggingEvent& event); 
};

class ZoneDateComponent: public FormatComponent
{
   public:
    virtual void append(std::ostringstream& out, const LoggingEvent& event);  
};
class LevelComponent : public FormatComponent 
{
public:
    virtual void append(std::ostringstream& out, const LoggingEvent& event); 
};

class ProcessIdComponent : public FormatComponent 
{
public:
    virtual void append(std::ostringstream& out, const LoggingEvent& event); 
};

class ThreadIdComponent : public FormatComponent 
{
public:
    virtual void append(std::ostringstream& out, const LoggingEvent& event); 
};

class FileComponent : public FormatComponent 
{
public:
    virtual void append(std::ostringstream& out, const LoggingEvent& event); 
};

class LineComponent : public FormatComponent 
{
public:
    virtual void append(std::ostringstream& out, const LoggingEvent& event); 
};

class FunctionComponent : public FormatComponent 
{
public:
    virtual void append(std::ostringstream& out, const LoggingEvent& event); 
};

}
#endif
