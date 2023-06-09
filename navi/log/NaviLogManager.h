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
#ifndef NAVI_NAVILOGMANAGER_H
#define NAVI_NAVILOGMANAGER_H

/*
copied from alog
 */

#include "navi/common.h"
#include "navi/config/LogConfig.h"
#include "navi/log/Sync.h"
#include "navi/log/AlogAppender.h"
#include "navi/log/Thread.h"
#include "aios/network/anet/atomic.h"
#include "autil/Lock.h"

namespace navi {

class NaviLogger;

/**
* @file     EventBase.h
* @brief    Deal with the asynchronous batch logging for FileAppenders
*
* @version  1.0.0
* @date     2013.1.12
* @author   yuejun.huyj
*/

class Condition {
public:
    Condition() {
        pthread_mutex_init(&m_mutex, NULL);
        pthread_cond_init(&m_cond, NULL);
    }

    ~Condition() {
        pthread_cond_destroy(&m_cond);
        pthread_mutex_destroy(&m_mutex);
    }

public:
    inline int lock() {
        return pthread_mutex_lock(&m_mutex);
    }

    inline int trylock () {
        return pthread_mutex_trylock(&m_mutex);
    }

    inline int unlock() {
        return pthread_mutex_unlock(&m_mutex);
    }

    inline int wait(int64_t ms)
    {
        timespec ts;
        ts.tv_sec = ms / 1000;
        ts.tv_nsec = (ms % 1000) * 1000000;
        return pthread_cond_timedwait(&m_cond, &m_mutex, &ts);
    }

    inline int signal() {
        return pthread_cond_signal(&m_cond);
    }

    inline int broadcast() {
        return pthread_cond_broadcast(&m_cond);
    }
private:
    pthread_mutex_t m_mutex;
    pthread_cond_t m_cond;
};

class NaviLogManager : public std::enable_shared_from_this<NaviLogManager>
{
public:
    NaviLogManager(InstanceId instanceId);
    ~NaviLogManager();
public:
    bool init(const LogConfig &config);
    void addFileAppender(const FileLogConfig &config);
    void collectFileAppenders();
    bool isRunning() { return m_bRunning; }
    void notify();
    // stop the worker I/O thread
    void stop();
    void flushFile(FileAppender* appender);
    std::shared_ptr<NaviLogger> createLogger(SessionId id = SessionId());
    int64_t flush(bool force);
    InstanceId getInstanceId() const;
private:
    void initInstanceId();
    SessionId getLoggerId();
    void addFileAppender(FileAppender *pFileAppender);
    void wait(uint64_t usec);
    static void worker_routine(void *pArg);
    void loop();
private:
    InstanceId _instanceId;
    atomic_t _logCounter;
    size_t _maxMessageLength;
    mutable autil::ReadWriteLock m_pendingMutex;
    Mutex m_flushMutex;
    std::vector<FileAppender *> m_pendingFileAppenders;
    std::vector<FileAppender *> m_fileAppenders;
    Condition m_condition;
    volatile bool m_bRunning;
    volatile bool m_notified;
    Thread m_worker;
private:
    static const int64_t s_iDefaultFlushIntervalInMS = 1000; // 1s
};

NAVI_TYPEDEF_PTR(NaviLogManager);

}

#endif //NAVI_NAVILOGMANAGER_H
