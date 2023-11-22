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
#include "navi/log/NaviLogManager.h"
#include "arpc/common/Exception.h"
#include "navi/log/Clock.h"
#include "navi/log/ConsoleAppender.h"
#include "navi/log/NaviLogger.h"
#include "autil/EnvUtil.h"
#include "navi/perf/NaviSymbolTable.h"
#include <execinfo.h>

namespace navi {

NaviLogManager::NaviLogManager(InstanceId instanceId) {
    _maxMessageLength = 1024;
    m_pendingFileAppenders.reserve(8);
    m_fileAppenders.reserve(8);
    m_bRunning = false;
    m_notified = false;
    _instanceId = instanceId;
    atomic_set(&_logCounter, 0);
}

NaviLogManager::~NaviLogManager()
{
    stop();
}

bool NaviLogManager::init(const LogConfig &config) {
    if (config.maxMessageLength > 0) {
        _maxMessageLength = config.maxMessageLength;
    } else {
        return false;
    }
    for (const auto &fileConfig : config.fileAppenders) {
        addFileAppender(fileConfig);
    }
    _neddConsoleLog = autil::EnvUtil::getEnv("console_log", false);
    return true;
}

void NaviLogManager::addFileAppender(const FileLogConfig &config) {
    auto appender = new FileAppender(config.levelStr, this, config.fileName.c_str());
    appender->setAutoFlush(config.autoFlush);
    appender->setMaxSize(config.maxFileSize);
    appender->setDelayTime(config.translateDelayTime());
    appender->setCompress(config.compress);
    appender->setCacheLimit(config.cacheLimit);
    appender->setHistoryLogKeepCount(config.logKeepCount);
    appender->setAsyncFlush(config.asyncFlush);
    appender->setFlushThreshold(config.flushThreshold * 1024);
    appender->setFlushIntervalInMS(config.flushInterval);

    for (const auto &btFilter : config.btFilters) {
        appender->addBtFilter(btFilter);
    }
    auto patternLayout = std::make_shared<PatternLayout>();
    patternLayout->setLogPattern(config.logPattern);
    appender->setLayout(patternLayout);

    addFileAppender(appender);
}

void NaviLogManager::addFileAppender(FileAppender *pFileAppender)
{
    assert(pFileAppender != NULL);
    autil::ScopedWriteLock guardLock(m_pendingMutex);
    m_pendingFileAppenders.push_back(pFileAppender);
    if (m_bRunning == false)
    {
        m_worker.start(worker_routine, this, "NaviLog");
        m_bRunning = true;
    }
    notify();
}

NaviLoggerPtr NaviLogManager::createLogger(SessionId id) {
    collectFileAppenders();
    auto logger = new NaviLogger(_maxMessageLength, shared_from_this());
    if (!id.valid()) {
        id = getLoggerId();
    } else {
        id.currentInstance = _instanceId;
    }
    autil::ScopedReadLock guardLock(m_pendingMutex);
    logger->init(id, m_fileAppenders);
    if (_neddConsoleLog) {
        logger->addAppender(std::make_shared<ConsoleAppender>("DEBUG"));
    }
    return NaviLoggerPtr(logger);
}

int64_t NaviLogManager::flush(bool force) {
    ScopedLock guardLock(m_flushMutex);
    int64_t now = Clock::nowMS();
    int64_t nextFlushTime = now + s_iDefaultFlushIntervalInMS;
    for (size_t i = 0; i < m_fileAppenders.size(); ++i) {
        int64_t flushtime =
            m_fileAppenders[i]->asyncFlush(now, force);
        if (nextFlushTime > flushtime) {
            nextFlushTime = flushtime;
        }
    }
    return nextFlushTime;
}

void NaviLogManager::loop()
{
    while (true)
    {
        collectFileAppenders();
        int64_t nexFlushTime = flush(false);
        wait(nexFlushTime);

        if (!m_bRunning) {
            return;
        }
    }

}

void NaviLogManager::collectFileAppenders()
{
    autil::ScopedWriteLock guardLock(m_pendingMutex);
    m_fileAppenders.insert(
            m_fileAppenders.end(),
            m_pendingFileAppenders.begin(),
            m_pendingFileAppenders.end());
    m_pendingFileAppenders.clear();
}

void NaviLogManager::stop()
{
    if (m_bRunning == true) {
        m_condition.lock();
        m_bRunning = false;
        m_condition.signal();
        m_condition.unlock();

        m_worker.stop();
    }

    collectFileAppenders();
    flush(true);

    {
        autil::ScopedWriteLock guardLock(m_pendingMutex);
        for (auto appender : m_pendingFileAppenders) {
            delete appender;
        }
        for (auto appender : m_fileAppenders) {
            delete appender;
        }
        m_pendingFileAppenders.clear();
        m_fileAppenders.clear();
    }
}

void NaviLogManager::notify()
{
    m_condition.lock();
    m_notified = true;
    m_condition.signal();
    m_condition.unlock();
}

void NaviLogManager::wait(uint64_t usec)
{
    m_condition.lock();
    if (!m_bRunning || m_notified) {
        m_notified = false;
        m_condition.unlock();
        return;
    }
    m_condition.wait(usec);
    m_notified = false;
    m_condition.unlock();
}

void NaviLogManager::worker_routine(void *pArg)
{
    ((NaviLogManager*) pArg)->loop();
}

void NaviLogManager::flushFile(FileAppender* appender)
{
    if (appender != NULL) {
        ScopedLock guardLock(m_flushMutex);
        appender->asyncFlush(Clock::nowMS(), true);
    }
}

SessionId NaviLogManager::getLoggerId() {
    SessionId id;
    id.instance = _instanceId;
    id.currentInstance = _instanceId;
    id.queryId = atomic_inc_return(&_logCounter);
    return id;
}

InstanceId NaviLogManager::getInstanceId() const {
    return _instanceId;
}

void NaviLogManager::setNaviSymbolTable(const NaviSymbolTable *naviSymbolTable) {
    _naviSymbolTable = naviSymbolTable;
}

std::string NaviLogManager::getCurrentBtString() const {
    if (_naviSymbolTable) {
        return getBtStringNavi();
    }
    arpc::common::Exception e;
    e.Init("", "", 0);
    return e.ToString();
}

std::string NaviLogManager::getBtStringNavi() const {
    constexpr int MAX_STACK_COUNT = 64;
    void *stackVec[MAX_STACK_COUNT];
    auto btCount = backtrace(stackVec, MAX_STACK_COUNT);
    std::string btStr;
    for (size_t i = 0; i < btCount; ++i) {
        auto ip = stackVec[i];
        btStr += _naviSymbolTable->resolveAddr((size_t)ip) + "\n";
    }
    return btStr;
}

std::string NaviLogManager::getExceptionMessage(const autil::legacy::ExceptionBase &e) const {
    auto btStr = e.GetExceptionHeader();
    if (!_naviSymbolTable) {
        return btStr;
    }
    size_t btCount = 0;
    auto stackVec = e.GetStacks(btCount);
    if (!stackVec || (0 == btCount)) {
        return btStr;
    }
    btStr += ", backtrace:\n";
    for (size_t i = 0; i < btCount; ++i) {
        auto ip = stackVec[i];
        btStr += _naviSymbolTable->resolveAddr((size_t)ip) + "\n";
    }
    return btStr;
}

}
