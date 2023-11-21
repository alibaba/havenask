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
#include "alog/Logger.h"

#include <assert.h>
#include <stdio.h>
#include <utility>

#include "LoggingEvent.h"
#include "EventBase.h"
#include "Sync.h"
#include "alog/Appender.h"

namespace alog {

// Global object of EventBase which provides the asynchronous batch
// logging functionality for FileAppenders.
EventBase gEventBase;

//static member init
std::string Logger::s_trashDir;
Mutex s_trashDirMutex;
Mutex s_rootMutex;
Mutex s_loggerMapMutex;
Logger* Logger :: s_rootLogger = NULL;
std::map<std::string, Logger *>* Logger::s_loggerMap = NULL;
int Logger::MAX_MESSAGE_LENGTH = 1024;

Logger::Logger(const char *name, uint32_t level, Logger *parent, bool bInherit)
    : m_appenderMutex(new RWMutex)
    , m_loggerName(name)
    , m_loggerLevel(level)
    , m_bLevelSet(false)
    , m_parent(parent)
    , m_bInherit(bInherit) {}

Logger* Logger::getRootLogger()
{
    ScopedLock lock(s_rootMutex);
    if (s_rootLogger == NULL)
    {
        s_rootLogger = new Logger("", LOG_LEVEL_INFO);
    }
    return s_rootLogger;
}

Logger* Logger::getLogger(const char *loggerName, bool bInherit )
{
    ScopedLock lock(s_loggerMapMutex);
    if (NULL == s_loggerMap) {
        s_loggerMap = new std::map<std::string, Logger *>();
    }
    return _getLogger(loggerName, bInherit);
}

void Logger::flushAll()
{
    if(NULL != s_loggerMap) {
        ScopedLock lock(s_loggerMapMutex);
        for (std::map<std::string, Logger *>::iterator it = s_loggerMap->begin();
             it != s_loggerMap->end(); it++)
        {
            it->second->flush();
        }
    }

    {
        ScopedLock lock2(s_rootMutex);
        if(s_rootLogger) {
            s_rootLogger->flush();
        }
    }
}

void Logger::flushCachedPidTid() {
    sysPid = getpid();
    sysTid = (long)syscall(SYS_gettid);
}

void Logger::shutdown()
{
    // Stop the I/O worker thread of gEventBase if it is started
    gEventBase.stop();

    //remove the root logger
    {
        ScopedLock lock(s_rootMutex);
        if (s_rootLogger != NULL)
        {
            delete s_rootLogger;
            s_rootLogger = NULL;
        }
    }

    //remove all the none root logger
    if (NULL != s_loggerMap) {
        ScopedLock lock(s_loggerMapMutex);
        for (std::map<std::string, Logger *>::const_iterator i = s_loggerMap->begin();
             i != s_loggerMap->end(); i++)
        {
            delete ((*i).second);
        }
        s_loggerMap->clear();
        delete s_loggerMap;
        s_loggerMap = NULL;
    }

    //release all the appenders
    Appender::release();
}

Logger::~Logger()
{
    removeAllAppenders();
    m_children.clear();
}

const std::string& Logger::getName() const
{
    return m_loggerName;
}

uint32_t Logger::getLevel() const
{
    return m_loggerLevel;
}

void Logger::setLevel(uint32_t level)
{
    if(level >= LOG_LEVEL_COUNT)
        return;
    m_loggerLevel = (level == LOG_LEVEL_NOTSET && m_bInherit && m_parent)? m_parent->getLevel() : level;
    if(m_loggerLevel == LOG_LEVEL_NOTSET)
        m_loggerLevel = LOG_LEVEL_INFO;
    m_bLevelSet = (level == LOG_LEVEL_NOTSET)? false : true;
    size_t childCnt = m_children.size();
    for(size_t i = 0; i < childCnt; i++) {
        m_children[i]->setLevelByParent(m_loggerLevel);
    }
}

void Logger::setLevelByParent(uint32_t level)
{
    if(level >= LOG_LEVEL_COUNT)
        return;
    if(!m_bInherit || m_bLevelSet)
        return;
    m_loggerLevel = (level == LOG_LEVEL_NOTSET)? LOG_LEVEL_INFO : level;
    size_t childCnt = m_children.size();
    for(size_t i = 0; i < childCnt; i++) {
        m_children[i]->setLevelByParent(m_loggerLevel);
    }
}

bool Logger::getInheritFlag() const
{
    return m_bInherit;
}

void Logger::setInheritFlag(bool bInherit)
{
    m_bInherit = bInherit;
    if(!m_bLevelSet && !m_bInherit)
    {
        m_loggerLevel = LOG_LEVEL_INFO;
        size_t childCnt = m_children.size();
        for(size_t i = 0; i < childCnt; i++) {
            m_children[i]->setLevelByParent(m_loggerLevel);
        }
    }
    if(!m_bLevelSet && m_bInherit)
    {
        m_loggerLevel = m_parent->getLevel();
        size_t childCnt = m_children.size();
        for(size_t i = 0; i < childCnt; i++) {
            m_children[i]->setLevelByParent(m_loggerLevel);
        }
    }
}

void Logger::setAppender(Appender* appender)
{
    if (appender)
    {
        removeAllAppenders() ;
        addAppender(appender);
    }
}

void Logger::addAppender(Appender* appender)
{
    if (appender)
    {
        {
            WScopedLock lock(*m_appenderMutex);
            std::set<Appender *>::iterator i = m_appenderSet.find(appender);
            if (m_appenderSet.end() == i)
            {
                m_appenderSet.insert(appender);
            }
        }
    }
}

void Logger::removeAllAppenders()
{
    {
        WScopedLock lock(*m_appenderMutex);
        m_appenderSet.clear();
    }
}
void Logger::logVaList(uint32_t level, const char *format, va_list ap) {
    if (__builtin_expect((!isLevelEnabled(level)),1))
        return;

    char *buffer = getTLSBufferStorage<Logger>();
    if (!buffer) {
        return;
    }
    vsnprintf(buffer, MAX_MESSAGE_LENGTH, format, ap);
    std::string msg(buffer);
    LoggingEvent event(m_loggerName, msg, level);

    _log(event);

}
void Logger::logVaList(uint32_t level, const char * file, int line, const char * func, const char* fmt, va_list ap)
{
    if (__builtin_expect((!isLevelEnabled(level)),1))
        return;

    char *buffer = getTLSBufferStorage<Logger>();
    if (!buffer) {
        return;
    }
    vsnprintf(buffer, MAX_MESSAGE_LENGTH, fmt, ap);
    std::string msg(buffer);
    LoggingEvent event(m_loggerName, msg, level, std::string(file), line, std::string(func));

    _log(event);
}

void Logger::log(uint32_t level, const char * file, int line, const char * func, const char* fmt, ...)
{
    if (__builtin_expect((!isLevelEnabled(level)),1))
        return;

    char *buffer = getTLSBufferStorage<Logger>();
    if (!buffer) {
        return;
    }

    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buffer, MAX_MESSAGE_LENGTH, fmt, ap);
    va_end(ap);
    std::string msg(buffer);
    LoggingEvent event(m_loggerName, msg, level, std::string(file), line, std::string(func));

    _log(event);
}

void Logger::log(uint32_t level, const char* fmt, ...)
{
    if (__builtin_expect((!isLevelEnabled(level)),1))
        return;

    char *buffer = getTLSBufferStorage<Logger>();
    if (!buffer) {
        return;
    }

    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buffer, MAX_MESSAGE_LENGTH, fmt, ap);
    va_end(ap);
    std::string msg(buffer);
    LoggingEvent event(m_loggerName, msg, level);

    _log(event);
}

void Logger::logPureMessage(uint32_t level, const char *message)
{
    if (__builtin_expect((!isLevelEnabled(level)),1))
        return;
    std::string msg(message);
    LoggingEvent event(m_loggerName, msg, level);

    _log(event);
}

void Logger::logBinaryMessage(uint32_t level, const std::string &message)
{
    if (__builtin_expect((!isLevelEnabled(level)),1))
        return;
    std::string msg(message);
    LoggingEvent event(m_loggerName, msg, level);

    _log(event);
}

void Logger::logBinaryMessage(uint32_t level, const char *message, size_t len)
{
    if (__builtin_expect((!isLevelEnabled(level)),1))
        return;
    std::string msg(message, len);
    LoggingEvent event(m_loggerName, msg, level);

    _log(event);
}

void Logger::flush()
{
    {
        RScopedLock lock(*m_appenderMutex);
        if (!m_appenderSet.empty())
        {
            for (std::set<Appender*>::const_iterator i = m_appenderSet.begin();
                 i != m_appenderSet.end(); i++)
            {
                (*i)->flush();
            }
        }
    }
}

/////////////////////private funtion///////////////////////////////////////////////////////////
void Logger::_log(LoggingEvent& event, int depth)
{
    if (m_bInherit && m_parent != NULL)
        m_parent->_log(event, depth+1);

    if(event.loggerName.size() == 0)
        event.loggerName = m_loggerName;
    {
        RScopedLock lock(*m_appenderMutex);
        if (!m_appenderSet.empty())
        {
            for (std::set<Appender*>::const_iterator i = m_appenderSet.begin();
                 i != m_appenderSet.end(); i++)
            {
                (*i)->append(event);
            }
        }
    }
    // if log is leaf node and FATAL
    if (0 == depth && alog::LOG_LEVEL_FATAL == event.level) {
        flushAll();
        abort();
    }
}

Logger* Logger::_getLogger(const char *loggerName, bool bInherit)
{
    Logger* result = NULL;
    std::string name = loggerName;
    assert(s_loggerMap);
    std::map<std::string, Logger *>::iterator i = s_loggerMap->find(name);
    if (s_loggerMap->end() != i)
    {
        result = (*i).second;
    }
    if (NULL == result)
    {
        std::string parentName;
        size_t dotIndex = name.find_last_of('.');
        Logger* parent = NULL;
        if (dotIndex == std::string::npos)
        {
            //the topest logger
            parent = getRootLogger();
        }
        else
        {
            //recursive get parent logger
            parentName = name.substr(0, dotIndex);
            parent = _getLogger(parentName.c_str());
        }
        result = new Logger(name.c_str(), parent->getLevel(), parent, bInherit);
        parent->m_children.push_back(result);
        s_loggerMap->insert(make_pair(name, result));
    }
    return result;
}

void Logger::setTrashDir(const std::string& dir) {
    ScopedLock guardLock(s_trashDirMutex);
    s_trashDir = dir;
}

std::string Logger::getTrashDir() {
    ScopedLock guardLock(s_trashDirMutex);
    return s_trashDir;
}

}
