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
#include <stdio.h>
#include <syslog.h>
#include <unistd.h>
#include <stdlib.h>
#include <limits.h>
#include <stdint.h>
#include <pwd.h>
#include <string>
#include <sstream> // IWYU pragma: keep
#include <fstream>
#include <list>
#include <iterator>
#include <map>
#include <stdexcept>
#include <utility>
#include <vector>

#include "alog/Configurator.h"
#include "StringUtil.h"
#include "Properties.h"
#include "EventBase.h"
#include "alog/Appender.h"
#include "alog/Layout.h"
#include "alog/Logger.h"

namespace alog {

extern EventBase gEventBase;

using namespace std;

typedef std::map<std::string, Appender*> AppenderMap;

void Configurator::configureLoggerFromString(const char* fileContent) noexcept(false) {
    std::istringstream is(fileContent);
    innerConfigureLogger(is);
}

void Configurator::configureLogger(const char *initFileName) noexcept(false)
{
    std::ifstream initFile(initFileName);
    if (!initFile)
    {
        throw std::runtime_error(std::string("Config File ") + initFileName + " does not exist or is unreadable");
    }
    innerConfigureLogger(initFile);
}

void Configurator::innerConfigureLogger(std::istream &is) {
    Properties properties;
    // parse the file to get all of the configuration
    properties.load(is);

    // global init
    globalInit(properties);

    AppenderMap allAppenders;
    initAllAppenders(allAppenders, properties);

    // get loggers
    std::vector<std::string> loggerList;
    initLoggers(loggerList, properties);

    // configure each logger
    for (std::vector<std::string>::const_iterator iter = loggerList.begin(); iter != loggerList.end(); ++iter)
    {
        configureLogger(*iter, allAppenders, properties);
    }
}

void Configurator::configureRootLogger()
{
    Logger* root = Logger::getRootLogger();
    root->setLevel(LOG_LEVEL_INFO);
    root->setAppender(ConsoleAppender::getAppender());
}

void Configurator::globalInit(Properties &properties)
{
    int value = properties.getInt(std::string("max_msg_len"), 1024);
    if(value > 0)
        Logger::MAX_MESSAGE_LENGTH = value;
}

void Configurator::initAllAppenders(AppenderMap &allAppenders, Properties &properties) noexcept(false)
{
    std::string currentAppender;

    //find all the "appender.XXX" key in properties map.
    std::string prefix("appender");
    Properties::const_iterator from = properties.lower_bound(prefix + '.');
    Properties::const_iterator to = properties.lower_bound(prefix + (char)('.' + 1));
    for (Properties::const_iterator i = from; i != to; ++i)
    {
        const std::string& key = (*i).first;
        std::list<std::string> propNameParts;
        std::back_insert_iterator<std::list<std::string> > pnpIt(propNameParts);
        StringUtil::split(pnpIt, key, '.');
        std::list<std::string>::const_iterator i2 = propNameParts.begin();
        std::list<std::string>::const_iterator iEnd = propNameParts.end();
        if (++i2 == iEnd)
        {
            throw std::runtime_error(std::string("missing appender name"));
        }

        const std::string appenderName = *i2++;

        /* WARNING, approaching lame code section:
           skipping of the Appenders properties only to get them
           again in instantiateAppender.
           */
        if (appenderName == currentAppender)
        {
            // simply skip properties for the current appender
        }
        else
        {
            if (i2 == iEnd)
            {
                // a new appender
                currentAppender = appenderName;
                allAppenders[currentAppender] = instantiateAppender(currentAppender,properties);
            }
            else
            {
                throw std::runtime_error(std::string("partial appender definition : ") + key);
            }
        }
    }
}

void Configurator::configureLogger(const std::string& loggerName, AppenderMap &allAppenders, Properties &properties) noexcept(false)
{
    // start by reading the "rootLogger" key
    std::string tempLoggerName =
        (loggerName == "rootLogger") ? loggerName : "logger." + loggerName;

    Properties::iterator iter = properties.find(tempLoggerName);

    if (iter == properties.end())
    {
        if (tempLoggerName == "rootLogger")
        {
            Logger::getRootLogger();
            return;
        }
        throw std::runtime_error(std::string("Unable to find logger: ") + tempLoggerName);
    }

    // need to get the root instance of the logger
    Logger* logger = (loggerName == "rootLogger") ?
                     Logger::getRootLogger() : Logger::getLogger(loggerName.c_str());


    std::list<std::string> tokens;
    std::back_insert_iterator<std::list<std::string> > tokIt(tokens);
    StringUtil::split(tokIt, (*iter).second, ',');
    std::list<std::string>::const_iterator i = tokens.begin();
    std::list<std::string>::const_iterator iEnd = tokens.end();

    uint32_t level = LOG_LEVEL_NOTSET;
    if (i != iEnd)
    {
        std::string  levelStr = StringUtil::trim(*i++);
        if (levelStr != "")
        {
            level = getLevelByString(levelStr);
            if (level >= LOG_LEVEL_NOTSET)
            {
                //level = LOG_LEVEL_INFO;
                i--;
                throw std::runtime_error("Unknow level  for logger '" + loggerName + "'");
            }
        }
    }
    logger->setLevel(level);

    bool inherit = properties.getBool("inherit." + loggerName, true);
    logger->setInheritFlag(inherit);

    logger->removeAllAppenders();
    for (/**/; i != iEnd; ++i)
    {
        std::string appenderName = StringUtil::trim(*i);
        AppenderMap::const_iterator appIt = allAppenders.find(appenderName);
        if (appIt == allAppenders.end())
        {
            // appender not found;
            throw std::runtime_error(std::string("Appender '") +
                                     appenderName + "' not found for logger '" + loggerName + "'");
        }
        else
        {
            /* pass by reference, i.e. don't transfer ownership
            */
            logger->addAppender(((*appIt).second));
        }
    }
}

Appender* Configurator::instantiateAppender(const std::string& appenderName, Properties &properties) noexcept(false)
{
    Appender* appender = NULL;
    std::string appenderPrefix = std::string("appender.") + appenderName;

    // determine the type by the appenderName
    Properties::iterator key = properties.find(appenderPrefix);
    if (key == properties.end())
        throw std::runtime_error(std::string("Appender '") + appenderName + "' not defined");

    std::string::size_type length = (*key).second.find_last_of(".");
    std::string appenderType = (length == std::string::npos) ?
                               (*key).second : (*key).second.substr(length+1);

    // and instantiate the appropriate object
    if (appenderType == "ConsoleAppender") {
        appender = ConsoleAppender::getAppender();
        bool isFlush = properties.getBool(appenderPrefix + ".flush", true);
        appender->setAutoFlush(isFlush);
    }
    else if (appenderType == "FileAppender") {
        std::string fileName = properties.getString(appenderPrefix + ".fileName", "default.log");
        fileName = transformPattern(fileName);
        appender = FileAppender::getAppender(fileName.c_str());
        bool isFlush = properties.getBool(appenderPrefix + ".flush", false);
        appender->setAutoFlush(isFlush);
	FileAppender *fileAppender = (FileAppender *)appender;
        uint32_t maxFileSize = properties.getInt(appenderPrefix + ".max_file_size", 0);
        fileAppender->setMaxSize(maxFileSize);
        uint32_t delayTime = translateDelayTime(properties.getString(appenderPrefix + ".delay_time", "0"));
        fileAppender->setDelayTime(delayTime);
        bool isCompress = properties.getBool(appenderPrefix + ".compress", false);
        fileAppender->setCompress(isCompress);
        uint32_t cacheLimit = properties.getInt(appenderPrefix + ".cache_limit", 0);
        fileAppender->setCacheLimit(cacheLimit);
        uint32_t keepCount = properties.getInt(appenderPrefix + ".log_keep_count", 0);
        fileAppender->setHistoryLogKeepCount(keepCount);
        bool isAsyncFlush = properties.getBool(appenderPrefix + ".async_flush", true);
        fileAppender->setAsyncFlush(isAsyncFlush);
        uint32_t flushThresholdInKB = properties.getInt(appenderPrefix + ".flush_threshold",
                                                    FileAppender::DEFAULT_FLUSH_THRESHOLD_IN_KB);
        if (flushThresholdInKB == 0) {
            fileAppender->setFlushThreshold(
                    FileAppender::DEFAULT_FLUSH_THRESHOLD_IN_KB);
        }
        else {
            fileAppender->setFlushThreshold(flushThresholdInKB * 1024);
        }
        uint32_t flushInterval = properties.getInt(appenderPrefix + ".flush_interval",
                                                   FileAppender::DEFAULT_FLUSH_INTERVAL_IN_MS);
        if (flushInterval == 0) {
            flushInterval = FileAppender::DEFAULT_FLUSH_INTERVAL_IN_MS;
        }
        fileAppender->setFlushIntervalInMS((uint64_t)flushInterval);

        if (fileAppender->isAutoFlush())
            fileAppender->setAsyncFlush(false);

        // If AsyncFlush is set on, then fileAppender is added into
        // EventBase for asynchronous flush event monitoring.
        if (fileAppender->isAsyncFlush())
            gEventBase.addFileAppender(fileAppender);
    }
    else if (appenderType == "SyslogAppender") {
        std::string ident = properties.getString(appenderPrefix + ".ident", "syslog");
        int facility = properties.getInt(appenderPrefix + ".facility", LOG_USER >> 3) << 3;
        appender = SyslogAppender::getAppender(ident.c_str(), facility);
    }
    else if(appenderType == "PipeAppender") {
        std::string fileName = properties.getString(appenderPrefix + ".fileName", "default.log");
        appender = PipeAppender::getAppender(fileName.c_str());
    }
    else {
        throw std::runtime_error(std::string("Appender '") + appenderName +
                                 "' has unknown type '" + appenderType + "'");
    }
    setLayout(appender, appenderName, properties);

    return appender;
}

void Configurator::setLayout(Appender* appender, const std::string& appenderName, Properties &properties)
{
    // determine the type by appenderName
    std::string tempString;
    Properties::iterator key = properties.find(std::string("appender.") + appenderName + ".layout");

    if (key == properties.end())
        return;

    std::string::size_type length = (*key).second.find_last_of(".");
    std::string layoutType = (length == std::string::npos) ? (*key).second : (*key).second.substr(length+1);

    Layout* layout;
    // and instantiate the appropriate object
    if (layoutType == "BasicLayout")
    {
        layout = new BasicLayout();
    }
    else if (layoutType == "SimpleLayout")
    {
        layout = new SimpleLayout();
    }
    else if (layoutType == "BinaryLayout")
    {
        layout = new BinaryLayout();
    }
    else if (layoutType == "PatternLayout")
    {
        PatternLayout* patternLayout = new PatternLayout();
        key = properties.find(std::string("appender.") + appenderName + ".layout.LogPattern");
        if (key == properties.end())
        {
            // leave default pattern
        }
        else
        {
            patternLayout->setLogPattern((*key).second);
        }
        layout = patternLayout;
    }
    else
    {
        throw std::runtime_error(std::string("Unknown layout type '" + layoutType + "' for appender '") + appenderName + "'");
    }

    appender->setLayout(layout);
}

/**
 * Get the categories contained within the map of properties.  Since
 * the logger looks something like "logger.xxxxx.yyy.zzz", we need
 * to search the entire map to figure out which properties are logger
 * listings.  Seems like there might be a more elegant solution.
 */
void Configurator::initLoggers(std::vector<std::string>& loggers, Properties &properties)
{
    loggers.clear();

    // add the root logger first
    loggers.push_back(std::string("rootLogger"));


    // then look for "logger."
    std::string prefix("logger");
    Properties::const_iterator from = properties.lower_bound(prefix + '.');
    Properties::const_iterator to = properties.lower_bound(prefix + (char)('.' + 1));
    for (Properties::const_iterator iter = from; iter != to; iter++) {
        loggers.push_back((*iter).first.substr(prefix.size() + 1));
    }
}

std::string Configurator::transformPattern(const std::string &inputPattern) {
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
        case '~':
            result.append(getHomeDirectory());
            break;
        default:
            result.append(1, '%');
            result.append(1, inputPattern[i]);
            break;
        }
    }
    return result;
}

std::string Configurator::getProgressName() {
    char buffer[64];
    sprintf(buffer, "/proc/%d/cmdline", getpid());
    ifstream fin(buffer);
    string line;
    if (!getline(fin, line)) {
        throw std::runtime_error(std::string("get line failed: filename[") + buffer + "]");
    }
    string procName = line.substr(0, line.find('\0'));
    size_t start = procName.rfind('/') + 1;
    size_t end = procName.length() - 1;
    return procName.substr(start, end - start + 1);
}

std::string Configurator::getLastDirectory(uint32_t layers) {
    char cwdPath[PATH_MAX];
    char* ret = getcwd(cwdPath, PATH_MAX);
    if (NULL == ret) {
        throw std::runtime_error(std::string("getcwd failed."));
    }
    string path = string(cwdPath);
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

std::string Configurator::getHomeDirectory() {
    const char *homedir = nullptr;
    if ((homedir = getenv("HOME")) == nullptr) {
        homedir = getpwuid(getuid())->pw_dir;
    }
    if (homedir != nullptr) {
        return std::string(homedir);
    }
    else {
        return std::string();
    }
}

uint32_t Configurator::getLevelByString(const std::string &levelStr)
{
    uint32_t level;
    if (levelStr == "DISABLE")
        level = LOG_LEVEL_DISABLE;
	else if (levelStr == "FATAL")
        level = LOG_LEVEL_FATAL;
    else if (levelStr == "ERROR")
        level = LOG_LEVEL_ERROR;
    else if (levelStr == "WARN")
        level = LOG_LEVEL_WARN;
    else if (levelStr == "INFO")
        level = LOG_LEVEL_INFO;
    else if (levelStr == "DEBUG")
        level = LOG_LEVEL_DEBUG;
    else if (levelStr == "TRACE1")
        level = LOG_LEVEL_TRACE1;
    else if (levelStr == "TRACE2")
        level = LOG_LEVEL_TRACE2;
    else if (levelStr == "TRACE3")
        level = LOG_LEVEL_TRACE3;
    else
        level = LOG_LEVEL_NOTSET;

    return level;

}

uint32_t Configurator::translateDelayTime(const std::string& str)
{
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

}
