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
#include "navi/log/Layout.h"
#include "navi/log/LoggingEvent.h"
#include "navi/log/FormatComponent.h"
#include <sstream>

namespace navi {

std::string BasicLayout::format(const LoggingEvent& event)
{
    constexpr int32_t timeLen = 24;
    char buffer[timeLen];
    event.formatTime(buffer, timeLen);

    std::ostringstream message;
    message << buffer << " " << event.levelStr() << " " << event.name
            << " : " << event.message << std::endl;
    return message.str();
}

std::string SimpleLayout::format(const LoggingEvent& event)
{
    constexpr int32_t timeLen = 24;
    char buffer[timeLen];
    event.formatTime(buffer, timeLen);

    std::ostringstream message;
    message << buffer << " " << event.levelStr() << " " << event.name
            << " (" << event.pid << "/" << event.tid << ") : " << event.message
            << std::endl;
    return message.str();
}

std::string BinaryLayout::format(const LoggingEvent& event)
{
    return event.message;
}

PatternLayout::PatternLayout() 
{
    std::string defaultPattern = "%%d %%l %%c (%%p/%%t) : %%m";
    setLogPattern(defaultPattern);
}

PatternLayout::~PatternLayout()
{
    clearLogPattern();
}

void PatternLayout::clearLogPattern() 
{
    for(ComponentVector::const_iterator i = m_components.begin(); i != m_components.end(); i++) 
    {
        delete (*i);
    }
    m_components.clear();
    m_logPattern = "";
}

void PatternLayout::setLogPattern(const std::string& logPattern)
{
    std::string literal;
    FormatComponent* component = NULL;
    clearLogPattern();
    for(std::string::const_iterator i = logPattern.begin(); i != logPattern.end(); i++)
    {
        if(*i == '%' && i+2 != logPattern.end())
        {
            char c = *(++i);
            if(c == '%')
            {
                char cc = *(++i);
                switch(cc)
                {
                    case 'm': component = new MessageComponent(); break;
                    case 'c': component = new NameComponent(); break;
                    case 'o': component = new ObjectComponent(); break;
                    case 'z': component = new PrefixComponent(); break;
                    case 'b': component = new BtComponent(); break;
                    case 'd': component = new DateComponent(); break;
                    case 'l': component = new LevelComponent(); break;
                    case 'p': component = new ProcessIdComponent(); break;
                    case 't': component = new ThreadIdComponent(); break;
                    case 'F': component = new FileComponent(); break;
                    case 'f': component = new FunctionComponent(); break;
                    case 'n': component = new LineComponent(); break;
                    default: literal += "%%"; literal += cc;
                }
                if(component)
                {
                    if (!literal.empty()) 
                    {
                        m_components.push_back(new StringComponent(literal));
                        literal = "";
                    }
                    m_components.push_back(component);
                    component = NULL;
                }
            }
            else
            {
                literal += '%';
                literal += c;
            }
        }
        else
        {
            literal += *i;
        }
    }
    if (!literal.empty()) 
    {
        m_components.push_back(new StringComponent(literal));
    }
    m_logPattern = logPattern;
}

std::string PatternLayout::format(const LoggingEvent& event) 
{
    std::ostringstream message;

    for(ComponentVector::const_iterator i = m_components.begin(); i != m_components.end(); i++)
    {
        (*i)->append(message, event);
    }
    message << std::endl;
    return message.str();
}

}
