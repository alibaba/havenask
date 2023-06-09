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
#include <cassert>

#include "EventBase.h"
#include "Clock.h"
#include "Sync.h"
#include "Thread.h"
#include "alog/Appender.h"

namespace alog
{

EventBase::EventBase()
{
    m_pendingFileAppenders.reserve(8);
    m_fileAppenders.reserve(8);
    m_bRunning = false;
    m_notified = false;
}

EventBase::~EventBase()
{
    stop();
}

void EventBase::addFileAppender(FileAppender *pFileAppender)
{
    assert(pFileAppender != NULL);
    ScopedLock guardLock(m_pendingMutex);
    m_pendingFileAppenders.push_back(pFileAppender);
    if (m_bRunning == false)
    {
        m_worker.start(worker_routine, this);
        m_bRunning = true;
    }
    notify();
}

int64_t EventBase::flush(bool force) {
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

void EventBase::loop()
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

void EventBase::collectFileAppenders()
{
    ScopedLock guardLock(m_pendingMutex);
    m_fileAppenders.insert(
            m_fileAppenders.end(),
            m_pendingFileAppenders.begin(),
            m_pendingFileAppenders.end());
    m_pendingFileAppenders.clear();
}

void EventBase::stop()
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
        ScopedLock guardLock(m_pendingMutex);
        m_pendingFileAppenders.clear();
        m_fileAppenders.clear();
    }
}

void EventBase::notify()
{
    m_condition.lock();
    m_notified = true;
    m_condition.signal();
    m_condition.unlock();
}

void EventBase::wait(uint64_t usec)
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

void EventBase::worker_routine(void *pArg)
{
    ((EventBase*) pArg)->loop();
}

void EventBase::flushFile(FileAppender* appender)
{
    if (appender != NULL) {
        ScopedLock guardLock(m_flushMutex);
        appender->asyncFlush(Clock::nowMS(), true);
    }
}

}
