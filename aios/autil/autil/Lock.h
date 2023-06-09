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

#include <pthread.h>
#include <stdlib.h>
#include <time.h>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <algorithm>
#include <limits>

#include "autil/CommonMacros.h"
#include "autil/ThreadAnnotations.h"
#include "autil/TimeUtility.h"

namespace autil {

#define ERRNO_FATAL(ret)                                                                                               \
    do {                                                                                                               \
        if (ret != 0) {                                                                                                \
            std::cerr << "errno: " << ret << std::endl;                                                                \
            abort();                                                                                                   \
        }                                                                                                              \
    } while (0)

class AUTIL_LOCKABLE ThreadMutex {
public:
    ThreadMutex(const pthread_mutexattr_t *mta = NULL) {
        int ret = pthread_mutex_init(&_mutex, mta);
        ERRNO_FATAL(ret);
    }

    ~ThreadMutex() { pthread_mutex_destroy(&_mutex); }

    int lock() AUTIL_EXCLUSIVE_LOCK_FUNCTION() { return pthread_mutex_lock(&_mutex); }

    int trylock() AUTIL_EXCLUSIVE_TRYLOCK_FUNCTION(false) { return pthread_mutex_trylock(&_mutex); }

    int unlock() AUTIL_UNLOCK_FUNCTION() { return pthread_mutex_unlock(&_mutex); }

private:
    ThreadMutex(const ThreadMutex &);
    ThreadMutex &operator=(const ThreadMutex &);

protected:
    pthread_mutex_t _mutex;
};

class AUTIL_LOCKABLE RecursiveThreadMutex : public ThreadMutex {
public:
    RecursiveThreadMutex() : ThreadMutex(RECURSIVE_PTHREAD_MUTEXATTR_PTR) {}

private:
    RecursiveThreadMutex(const RecursiveThreadMutex &);
    RecursiveThreadMutex &operator=(const RecursiveThreadMutex &);

private:
    static const pthread_mutexattr_t *RECURSIVE_PTHREAD_MUTEXATTR_PTR;
};

class AUTIL_LOCKABLE ThreadCond : public ThreadMutex {
public:
    ThreadCond() {
        int ret = pthread_cond_init(&_cond, NULL);
        ERRNO_FATAL(ret);
    }

    ~ThreadCond() { pthread_cond_destroy(&_cond); }

    virtual int wait(int64_t usec = 0) {
        int ret = 0;
        if (usec <= 0) {
            ret = pthread_cond_wait(&_cond, &_mutex);
        } else {
            timespec ts = TimeUtility::getTimespec(usec);
            ret = pthread_cond_timedwait(&_cond, &_mutex, &ts);
        }

        return ret;
    }

    virtual int signal() { return pthread_cond_signal(&_cond); }

    int broadcast() { return pthread_cond_broadcast(&_cond); }

protected:
    pthread_cond_t _cond;
};

class ThreadCountedCond : public ThreadCond {
public:
    ThreadCountedCond() : ThreadCond(), _count(0) {}
    ~ThreadCountedCond() {}
    int waitCounted(int64_t usec = 0) {
        lock();
        int ret = 0;
        while (_count <= 0) {
            ret = wait(usec);
        }
        --_count;
        unlock();
        return ret;
    }
    virtual int signalCounted() {
        lock();
        ++_count;
        int ret = signal();
        unlock();
        return ret;
    }

protected:
    int64_t _count;
};

class AUTIL_LOCKABLE ProducerConsumerCond : public ThreadMutex {
public:
    ProducerConsumerCond() {
        int ret = pthread_cond_init(&_producerCond, NULL);
        ERRNO_FATAL(ret);
        ret = pthread_cond_init(&_consumerCond, NULL);
        ERRNO_FATAL(ret);
    }

    ~ProducerConsumerCond() {
        pthread_cond_destroy(&_producerCond);
        pthread_cond_destroy(&_consumerCond);
    }

public:
    int producerWait(int64_t usec = 0) { return wait(_producerCond, usec); }

    int signalProducer() { return signal(_producerCond); }

    int broadcastProducer() { return broadcast(_producerCond); }

    int consumerWait(int64_t usec = 0) { return wait(_consumerCond, usec); }

    int signalConsumer() { return signal(_consumerCond); }

    int broadcastConsumer() { return broadcast(_consumerCond); }

private:
    int wait(pthread_cond_t &cond, int64_t usec) {
        int ret = 0;
        if (usec <= 0) {
            ret = pthread_cond_wait(&cond, &_mutex);
        } else {
            timespec ts = TimeUtility::getTimespec(usec);
            ret = pthread_cond_timedwait(&cond, &_mutex, &ts);
        }

        return ret;
    }

    static int signal(pthread_cond_t &cond) { return pthread_cond_signal(&cond); }

    static int broadcast(pthread_cond_t &cond) { return pthread_cond_broadcast(&cond); }

protected:
    pthread_cond_t _producerCond;
    pthread_cond_t _consumerCond;
};

class AUTIL_SCOPED_LOCKABLE ScopedLock {
private:
    ThreadMutex &_lock;

private:
    ScopedLock(const ScopedLock &);
    ScopedLock &operator=(const ScopedLock &);

public:
    explicit ScopedLock(ThreadMutex &lock) AUTIL_EXCLUSIVE_LOCK_FUNCTION(lock) : _lock(lock) {
        int ret = _lock.lock();
        ERRNO_FATAL(ret);
    }

    ~ScopedLock() AUTIL_UNLOCK_FUNCTION() {
        int ret = _lock.unlock();
        ERRNO_FATAL(ret);
    }
};

class AUTIL_LOCKABLE ReadWriteLock {
private:
    ReadWriteLock(const ReadWriteLock &);
    ReadWriteLock &operator=(const ReadWriteLock &);

public:
    enum Mode
    {
        PREFER_READER,
        PREFER_WRITER
    };

    ReadWriteLock(Mode mode = PREFER_READER) {
        pthread_rwlockattr_t attr;
        int ret = pthread_rwlockattr_init(&attr);
        ERRNO_FATAL(ret);
        switch (mode) {
        case PREFER_WRITER:
            pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
            break;
        case PREFER_READER:
            pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_READER_NP);
            break;
        default:
            break;
        }
        ret = pthread_rwlock_init(&_lock, &attr);
        ERRNO_FATAL(ret);
    }

    ~ReadWriteLock() { pthread_rwlock_destroy(&_lock); }

    int rdlock() AUTIL_SHARED_LOCK_FUNCTION() { return pthread_rwlock_rdlock(&_lock); }

    int wrlock() AUTIL_EXCLUSIVE_LOCK_FUNCTION() { return pthread_rwlock_wrlock(&_lock); }

    int tryrdlock() AUTIL_SHARED_TRYLOCK_FUNCTION(false) { return pthread_rwlock_tryrdlock(&_lock); }

    int trywrlock() AUTIL_EXCLUSIVE_TRYLOCK_FUNCTION(false) { return pthread_rwlock_trywrlock(&_lock); }

    int unlock() AUTIL_UNLOCK_FUNCTION() { return pthread_rwlock_unlock(&_lock); }

protected:
    pthread_rwlock_t _lock;
};

class AUTIL_SCOPED_LOCKABLE ScopedReadLock {
private:
    ReadWriteLock &_lock;

private:
    ScopedReadLock(const ScopedReadLock &);
    ScopedReadLock &operator=(const ScopedReadLock &);

public:
    explicit ScopedReadLock(ReadWriteLock &lock) AUTIL_SHARED_LOCK_FUNCTION(lock) : _lock(lock) {
        int ret = _lock.rdlock();
        ERRNO_FATAL(ret);
    }
    ~ScopedReadLock() AUTIL_UNLOCK_FUNCTION() {
        int ret = _lock.unlock();
        ERRNO_FATAL(ret);
    }
};

class AUTIL_SCOPED_LOCKABLE ScopedWriteLock {
private:
    ReadWriteLock &_lock;

private:
    ScopedWriteLock(const ScopedWriteLock &);
    ScopedWriteLock &operator=(const ScopedWriteLock &);

public:
    explicit ScopedWriteLock(ReadWriteLock &lock) AUTIL_EXCLUSIVE_LOCK_FUNCTION(lock) : _lock(lock) {
        int ret = _lock.wrlock();
        ERRNO_FATAL(ret);
    }
    ~ScopedWriteLock() AUTIL_UNLOCK_FUNCTION() {
        int ret = _lock.unlock();
        ERRNO_FATAL(ret);
    }
};

class AUTIL_SCOPED_LOCKABLE ScopedReadWriteLock {
private:
    ReadWriteLock &_lock;
    char _mode;

private:
    ScopedReadWriteLock(const ScopedReadWriteLock &);
    ScopedReadWriteLock &operator=(const ScopedReadWriteLock &);

public:
    // annotate it as exclusive lock for now, no way to annotate a lock with mode as parameter.
    explicit ScopedReadWriteLock(ReadWriteLock &lock, const char mode) AUTIL_EXCLUSIVE_LOCK_FUNCTION(lock)
        : _lock(lock), _mode(mode) {
        if (_mode == 'r' || _mode == 'R') {
            int ret = _lock.rdlock();
            ERRNO_FATAL(ret);
        } else if (_mode == 'w' || _mode == 'W') {
            int ret = _lock.wrlock();
            ERRNO_FATAL(ret);
        }
    }

    ~ScopedReadWriteLock() AUTIL_UNLOCK_FUNCTION() {
        if (_mode == 'r' || _mode == 'R' || _mode == 'w' || _mode == 'W') {
            int ret = _lock.unlock();
            ERRNO_FATAL(ret);
        }
    }
};

class Notifier {
public:
    static const int EXITED;

public:
    Notifier(int accumulatedNotificationMax = std::numeric_limits<int>::max())
        : _accumulatedNotification(0)
        , _exitFlag(false)
        , _accumulatedNotificationMax(accumulatedNotificationMax)
    {}

    int notifyExit() {
        ScopedLock lock(_cond);
        _exitFlag = true;
        return _cond.broadcast();
    }

    int notify() {
        ScopedLock lock(_cond);
        if (++_accumulatedNotification > _accumulatedNotificationMax) {
            _accumulatedNotification = _accumulatedNotificationMax;
            return 0;
        } else {
            return _cond.signal();
        }
    }

    bool isExit() const {
        ScopedLock lock(_cond);
        return _exitFlag;
    }

    /**
     * return 0 if got notified, and
     * return errno or EXITED
     */
    int waitNotification(int timeout = -1) { return wait(timeout); }

private:
    mutable ThreadCond _cond;
    int _accumulatedNotification;
    bool _exitFlag;
    int _accumulatedNotificationMax;

private:
    int wait(int timeout = 0) {
        ScopedLock lock(_cond);
        while (true) {
            if (_exitFlag) {
                return EXITED;
            }

            if (_accumulatedNotification > 0) {
                _accumulatedNotification = _accumulatedNotification - 1;
                return 0;
            }

            int ret = _cond.wait(timeout);
            if (ret != 0) {
                return ret;
            }
        }
    }
};

class TerminateClosure {
public:
    TerminateClosure() {}
    virtual ~TerminateClosure() {}

public:
    virtual void Run() = 0;
};

class TerminateNotifier {
public:
    static const int EXITED = (1 << 16) + 1;

public:
    TerminateNotifier() : _count(0), _exitFlag(false), _closure(NULL) {}
    /***
        dec must be called with inc in pair.
        And dec must be called after inc.
     */
    int inc() {
        autil::ScopedLock lock(_cond);
        _count = _count + 1;
        return 0;
    }
    int dec() {
        TerminateClosure *closure = NULL;
        int ret = 0;
        // get closure out of the lock range
        // to prevent "Run" delete "this"
        {
            // ENTER LOCK RANGE
            autil::ScopedLock lock(_cond);
            _count = _count - 1;
            assert(_count >= 0);
            if (_count == 0) {
                if (_closure) {
                    closure = _closure;
                    _closure = NULL;
                }
                ret = _cond.broadcast();
            }
            // LEAVE LOCK RANGE
        }

        if (closure) {
            closure->Run();
        }
        return ret;
    }
    /**
     return 0 if got notified, and
     return errno or EXITED
     ether onTerminate or wait can be called, but not together
     */
    int wait(int timeout = -1) {
        autil::ScopedLock lock(_cond);
        while (true) {
            if (_count == 0) {
                return 0;
            }

            if (_exitFlag) {
                return EXITED;
            }

            int ret = _cond.wait(timeout);
            if (ret != 0) {
                return ret;
            }
        }
    }
    int peek() {
        autil::ScopedLock lock(_cond);
        return _count;
    }
    /*
      After onTerminate, NO MORE inc and dec should be called.
      The closure will be call only once.
      The closure MUST be self deleted or deleted by outer caller.

      ether onTerminate or wait can be called, but not together
    */
    int onTerminate(TerminateClosure *closure) {
        {
            // ENTER LOCK RANGE
            autil::ScopedLock lock(_cond);
            if ((_count == 0) && (closure)) {
                // event happen, and the new closure is not NULL
                // new closure should be called right now
                _closure = NULL;
            } else {
                // event not happen yet, or NULL is set
                // simply save it
                _closure = closure;
                return 0;
            }
            // LEAVE LOCK RANGE
        }

        if (closure) {
            closure->Run();
        }
        return 0;
    }
    /*
        int notifyExit()
        {
            autil::ScopedLock lock(_cond);
            _exitFlag = true;
            // FIXME: may not alloc by new
            if (_closure) {
                delete _closure;
                _closure = NULL;
            }
            return _cond.broadcast();
        }
    */
private:
    volatile int _count;
    volatile bool _exitFlag;
    autil::ThreadCond _cond;
    TerminateClosure *_closure;
};

class TicketSpinLock {
public:
    TicketSpinLock() : users(0), ticket(0) {}
    ~TicketSpinLock() {}

private:
    TicketSpinLock(const TicketSpinLock &) = delete;
    TicketSpinLock &operator=(const TicketSpinLock &) = delete;

public:
    std::atomic<uint16_t> users;
    std::atomic<uint16_t> ticket;
};

class ScopedTicketSpinLock {
public:
    ScopedTicketSpinLock(TicketSpinLock &lock_) : lock(lock_) {
        auto me = lock.users++;
        while (unlikely(lock.ticket.load() != me))
#if __x86_64__
            asm volatile("rep;nop" : : : "memory");
#elif __aarch64__
#define __nops(n) ".rept " #n "\nnop\n.endr\n"
            asm volatile(__nops(1));
#else
#error arch unsupported!
#endif
    }
    ~ScopedTicketSpinLock() { ++lock.ticket; }

private:
    ScopedTicketSpinLock(const TicketSpinLock &) = delete;
    ScopedTicketSpinLock &operator=(const TicketSpinLock &) = delete;

private:
    TicketSpinLock &lock;
};

class AUTIL_LOCKABLE SpinLock {
public:
    void lock() AUTIL_EXCLUSIVE_LOCK_FUNCTION() {
        while (flag.test_and_set(std::memory_order_acquire))
            ;
    }
    void unlock() AUTIL_UNLOCK_FUNCTION() { flag.clear(std::memory_order_release); }

private:
    std::atomic_flag flag = ATOMIC_FLAG_INIT;
};

class AUTIL_SCOPED_LOCKABLE ScopedSpinLock {
public:
    ScopedSpinLock(SpinLock &lock) AUTIL_EXCLUSIVE_LOCK_FUNCTION(lock) : _lock(lock) { _lock.lock(); }
    ~ScopedSpinLock() AUTIL_UNLOCK_FUNCTION() { _lock.unlock(); }

private:
    ScopedSpinLock(const ScopedSpinLock &) = delete;
    ScopedSpinLock &operator=(const ScopedSpinLock &) = delete;

private:
    SpinLock &_lock;
};

#undef ERRNO_FATAL

} // namespace autil
