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
#ifndef FUTURE_LITE_CORO_SHARED__MUTEXH
#define FUTURE_LITE_CORO_SHARED__MUTEXH

#include <atomic>
#include <cassert>
#include <limits>
#include <mutex>
#include <utility>

#include "future_lite/Common.h"
#include "future_lite/experimental/coroutine.h"
#include "future_lite/coro/SharedLock.h"
#include "future_lite/coro/SpinLock.h"

namespace future_lite {
namespace coro {

/// The SharedMutex class provides a thread synchronisation
/// primitive that allows a coroutine to asynchronously acquire a lock on the
/// mutex.
///
/// The mutex supports two kinds of locks:
/// - exclusive-lock - Also known as a write-lock.
///                    While an exclusive lock is held, no other thread will be
///                    able to acquire either an exclusive lock or a shared
///                    lock until the exclusive lock is released.
/// - shared-lock    - Also known as a read-lock.
///                    The mutex permits multiple shared locks to be held
///                    concurrently but does not permit shared locks to be held
///                    concurrently with exclusive locks.
///
/// This mutex employs a fair lock acquisition strategy that attempts to process
/// locks in a mostly FIFO order in which they arrive at the mutex.
/// This means that if the mutex is currently read-locked and some coroutine
/// tries to acquire a write-lock, that subsequent read-lock attempts will
/// be queued up behind the write-lock, allowing the write-lock to be acquired
/// in a bounded amount of time.
///
/// One implication of this strategy is that it is not safe to unconditionally
/// acquire a new read-lock while already holding a read-lock, since it's
/// possible that this could lead to deadlock if there was another coroutine
/// that was currently waiting on a write-lock.
///
/// The locks acquired by this mutex do not have thread affinity. A coroutine
/// can acquire the lock on one thread and release the lock on another thread.
///
/// Example usage:
///
///  class AsyncStringSet {
//     using namespace future_lite::coro;
///    mutable SharedMutex _mutex;
///    std::unordered_set<std::string> values_;
///
///    AsyncStringSet() = default;
///
///    Lazy<bool> insert(std::string value) {
///      auto lock = co_await _mutex.coScopedLock();
///      co_return values_.insert(value).second;
///    }
///
///    Lazy<bool> remove(std::string value) {
///      auto lock = co_await _mutex.coScopedLock();
///      co_return values_.erase(value) > 0;
///    }
///
///    Lazy<bool> contains(std::string value) const {
///      auto lock = co_await _mutex.coScopedSharedLock();
///      co_return values_.count(value) > 0;
///    }
///  }

class SharedMutex {
private:
    template <typename Awaiter>
    class LockOperation;
    class LockAwaiter;
    class ScopedLockAwaiter;
    class LockSharedAwaiter;
    class ScopedLockSharedAwaiter;

public:
    SharedMutex() noexcept = default;

    ~SharedMutex();

    /// Try to acquire an exclusive lock on the mutex synchronously.
    ///
    /// If this returns true then the exclusive lock was acquired synchronously
    /// and the caller is responsible for calling .unlock() later to release
    /// the exclusive lock. If this returns false then the lock was not acquired.
    ///
    /// Consider using a std::unique_lock to ensure the lock is released at the
    /// end of a scope.
    bool tryLock() noexcept;

    /// Try to acquire a shared lock on the mutex synchronously.
    ///
    /// If this returns true then the shared lock was acquired synchronously
    /// and the caller is responsible for calling .unlockShared() later to
    /// release the shared lock.
    bool tryLockShared() noexcept;

    /// Asynchronously acquire an exclusive lock on the mutex.
    ///
    /// Returns a SemiAwaitable<void> type that requires the caller to inject
    /// an executor by calling .viaIfAsync(executor) and then co_awaiting the
    /// result to wait for the lock to be acquired. Note that if you are awaiting
    /// the lock operation within a Lazy then the current executor
    /// will be injected implicitly without needing to call .viaIfAsync().
    ///
    /// If the lock was acquired synchronously then the awaiting coroutine
    /// continues on the current thread without suspending.
    /// If the lock could not be acquired synchronously then the awaiting
    /// coroutine is suspended and later resumed on the specified executor when
    /// the lock becomes available.
    ///
    /// After this operation completes, the caller is responsible for calling
    /// .unlock() to release the lock.
    [[nodiscard]] LockOperation<LockAwaiter> coLock() noexcept;

    /// Asynchronously acquire an exclusive lock on the mutex and return an object
    /// that will release the lock when it goes out of scope.
    ///
    /// Returns a SemiAwaitable<std::unique_lock<SharedMutex>> that, once
    /// associated with an executor using .viaIfAsync(), must be co_awaited to
    /// wait for the lock to be acquired.
    ///
    /// If the lock could be acquired immediately then the coroutine continues
    /// execution without suspending. Otherwise, the coroutine is suspended and
    /// will later be resumed on the specified executor once the lock has been
    /// acquired.
    [[nodiscard]] LockOperation<ScopedLockAwaiter> coScopedLock() noexcept;

    /// Asynchronously acquire a shared lock on the mutex.
    ///
    /// Returns a SemiAwaitable<void> type that requires the caller to inject
    /// an executor by calling .viaIfAsync(executor) and then co_awaiting the
    /// result to wait for the lock to be acquired. Note that if you are awaiting
    /// the lock operation within a Lazy then the current executor
    /// will be injected implicitly without needing to call .viaIfAsync().
    ///
    /// If the lock was acquired synchronously then the awaiting coroutine
    /// continues on the current thread without suspending.
    /// If the lock could not be acquired synchronously then the awaiting
    /// coroutine is suspended and later resumed on the specified executor when
    /// the lock becomes available.
    ///
    /// After this operation completes, the caller is responsible for calling
    /// .unlockShared() to release the lock.
    [[nodiscard]] LockOperation<LockSharedAwaiter> coLockShared() noexcept;

    /// Asynchronously acquire an exclusive lock on the mutex and return an object
    /// that will release the lock when it goes out of scope.
    ///
    /// Returns a SemiAwaitable<std::shared_lock<SharedMutex>> that, once
    /// associated with an executor using .viaIfAsync(), must be co_awaited to
    /// wait for the lock to be acquired.
    ///
    /// If the lock could be acquired immediately then the coroutine continues
    /// execution without suspending. Otherwise, the coroutine is suspended and
    /// will later be resumed on the specified executor once the lock has been
    /// acquired.
    [[nodiscard]] LockOperation<ScopedLockSharedAwaiter>
    coScopedLockShared() noexcept;

    /// Release the exclusive lock.
    ///
    /// This will resume the next coroutine(s) waiting to acquire the lock, if
    /// any.
    void unlock() noexcept;

    /// Release a shared lock.
    ///
    /// If this is the last shared lock then this will resume the next
    /// coroutine(s) waiting to acquire the lock, if any.
    void unlockShared() noexcept;

private:
    enum class LockType : std::uint8_t { EXCLUSIVE, SHARED };

    class LockAwaiterBase {
    protected:
        friend class SharedMutex;

        explicit LockAwaiterBase(SharedMutex& mutex, LockType lockType) noexcept
            : _mutex(&mutex), _nextAwaiter(nullptr), _lockType(lockType) {}

        void resume() noexcept {
            _continuation.resume();
        }

        SharedMutex* _mutex;
        LockAwaiterBase* _nextAwaiter;
        LockAwaiterBase* _nextReader;
        std::coroutine_handle<> _continuation;
        LockType _lockType;
    };

    class LockAwaiter : public LockAwaiterBase {
    public:
        explicit LockAwaiter(SharedMutex& mutex) noexcept
            : LockAwaiterBase(mutex, LockType::EXCLUSIVE) {}

        bool await_ready() noexcept {
            return _mutex->tryLock();
        }

        bool await_suspend(
            std::coroutine_handle<> continuation) noexcept {
            ScopedSpinLock lock(_mutex->_state._accessLock);
            // Exclusive lock can only be acquired if it's currently unlocked.
            if (_mutex->_state._lockedFlagAndReaderCount == kUnlocked) {
                _mutex->_state._lockedFlagAndReaderCount = kExclusiveLockFlag;
                return false;
            }
            // Append to the end of the waiters queue.
            _continuation = continuation;
            *_mutex->_state._waitersTailNext = this;
            _mutex->_state._waitersTailNext = &_nextAwaiter;
            return true;
        }

        void await_resume() noexcept {}
    };

    class LockSharedAwaiter : public LockAwaiterBase {
    public:
        explicit LockSharedAwaiter(SharedMutex& mutex) noexcept
            : LockAwaiterBase(mutex, LockType::SHARED) {}

        bool await_ready() noexcept {
            return _mutex->tryLockShared();
        }

        bool await_suspend(
            std::coroutine_handle<> continuation) noexcept {
            ScopedSpinLock lock(_mutex->_state._accessLock);
            // shared-lock can be acquired if it's either unlocked or it is
            // currently locked shared and there is no queued waiters.
            if (_mutex->_state._lockedFlagAndReaderCount == kUnlocked ||
                (_mutex->_state._lockedFlagAndReaderCount != kExclusiveLockFlag &&
                 _mutex->_state._waitersHead == nullptr)) {
                _mutex->_state._lockedFlagAndReaderCount += kSharedLockCountIncrement;
                return false;
            }
            // Lock not available immediately.
            // Queue up for later resumption.
            _continuation = continuation;
            *_mutex->_state._waitersTailNext = this;
            _mutex->_state._waitersTailNext = &_nextAwaiter;
            return true;
        }

        void await_resume() noexcept {}
    };

    class ScopedLockAwaiter : public LockAwaiter {
    public:
        using LockAwaiter::LockAwaiter;

        [[nodiscard]] std::unique_lock<SharedMutex> await_resume() noexcept {
            LockAwaiter::await_resume();
            return std::unique_lock<SharedMutex>{*_mutex, std::adopt_lock};
        }
    };

    class ScopedLockSharedAwaiter : public LockSharedAwaiter {
    public:
        using LockSharedAwaiter::LockSharedAwaiter;

        [[nodiscard]] SharedLock<SharedMutex> await_resume() noexcept {
            LockSharedAwaiter::await_resume();
            return SharedLock<SharedMutex>{*_mutex, std::adopt_lock};
        }
    };

    friend class LockAwaiter;

    template <typename Awaiter>
    class LockOperation {
    public:
        explicit LockOperation(SharedMutex& mutex) noexcept : _mutex(mutex) {
            _awaiter = new Awaiter(mutex);
        }

        LockOperation(const LockOperation& other) : _mutex(other._mutex) {
            this->_awaiter = other._awaiter;
            other._awaiter = nullptr;
        }

        ~LockOperation() {
            delete _awaiter;
        }

        bool await_ready() noexcept {
            return _awaiter->await_ready();
        }

        bool await_suspend(
            std::coroutine_handle<> awaitingCoroutine) noexcept {
            return _awaiter->await_suspend(awaitingCoroutine);
        }

        void await_resume() noexcept {
            _awaiter->await_resume();
        }

    private:
        SharedMutex& _mutex;
        mutable Awaiter* _awaiter;
    };

    struct State {
        State() noexcept
        : _lockedFlagAndReaderCount(kUnlocked),
            _waitersHead(nullptr),
            _waitersTailNext(&_waitersHead) {}

        // bit 0          - locked by writer
        // bits 1-[31/63] - reader lock count
        std::size_t _lockedFlagAndReaderCount;
        LockAwaiterBase* _waitersHead;
        LockAwaiterBase** _waitersTailNext;
        SpinLock _accessLock;
    };

    static LockAwaiterBase* unlockOrGetNextWaitersToResume(State& state) noexcept;

    static void resumeWaiters(LockAwaiterBase* awaiters) noexcept;

    static constexpr std::size_t kUnlocked = 0;
    static constexpr std::size_t kExclusiveLockFlag = 1;
    static constexpr std::size_t kSharedLockCountIncrement = 2;

    State _state;
};

inline SharedMutex::LockOperation<SharedMutex::LockAwaiter>
SharedMutex::coLock() noexcept {
    return LockOperation<LockAwaiter>{*this};
}

inline SharedMutex::LockOperation<SharedMutex::LockSharedAwaiter>
SharedMutex::coLockShared() noexcept {
    return LockOperation<LockSharedAwaiter>{*this};
}

inline SharedMutex::LockOperation<SharedMutex::ScopedLockAwaiter>
SharedMutex::coScopedLock() noexcept {
    return LockOperation<ScopedLockAwaiter>{*this};
}

inline SharedMutex::LockOperation<SharedMutex::ScopedLockSharedAwaiter>
SharedMutex::coScopedLockShared() noexcept {
    return LockOperation<ScopedLockSharedAwaiter>{*this};
}


inline SharedMutex::~SharedMutex() {
    assert(_state._lockedFlagAndReaderCount == kUnlocked);
    assert(_state._waitersHead == nullptr);
}

inline bool SharedMutex::tryLock() noexcept {
    ScopedSpinLock lock(_state._accessLock);
    if (_state._lockedFlagAndReaderCount == kUnlocked) {
        _state._lockedFlagAndReaderCount = kExclusiveLockFlag;
        return true;
    }
    return false;
}

inline bool SharedMutex::tryLockShared() noexcept {
    ScopedSpinLock lock(_state._accessLock);
    if (_state._lockedFlagAndReaderCount == kUnlocked ||
        (_state._lockedFlagAndReaderCount >= kSharedLockCountIncrement &&
         _state._waitersHead == nullptr)) {
        _state._lockedFlagAndReaderCount += kSharedLockCountIncrement;
        return true;
    }
    return false;
}

inline void SharedMutex::unlock() noexcept {
    LockAwaiterBase* awaitersToResume = nullptr;
    {
        ScopedSpinLock lock(_state._accessLock);
        assert(_state._lockedFlagAndReaderCount == kExclusiveLockFlag);
        awaitersToResume = unlockOrGetNextWaitersToResume(_state);
    }
    resumeWaiters(awaitersToResume);
}

inline void SharedMutex::unlockShared() noexcept {
    LockAwaiterBase* awaitersToResume = nullptr;
    {
        ScopedSpinLock lock(_state._accessLock);
        assert(_state._lockedFlagAndReaderCount >= kSharedLockCountIncrement);
        _state._lockedFlagAndReaderCount -= kSharedLockCountIncrement;
        if (_state._lockedFlagAndReaderCount != kUnlocked) {
            return;
        }
        awaitersToResume = unlockOrGetNextWaitersToResume(_state);
    }
    resumeWaiters(awaitersToResume);
}

inline SharedMutex::LockAwaiterBase*
SharedMutex::unlockOrGetNextWaitersToResume(SharedMutex::State& state) noexcept {
    auto* head = state._waitersHead;
    if (head != nullptr) {
        if (head->_lockType == LockType::EXCLUSIVE) {
            state._waitersHead = std::exchange(head->_nextAwaiter, nullptr);
            state._lockedFlagAndReaderCount = kExclusiveLockFlag;
        } else {
            std::size_t newState = kSharedLockCountIncrement;
            // Scan for a run of SHARED lock types.
            auto* last = head;
            auto* next = last->_nextAwaiter;
            while (next != nullptr && next->_lockType == LockType::SHARED) {
                last = next;
                next = next->_nextAwaiter;
                newState += kSharedLockCountIncrement;
            }
            last->_nextAwaiter = nullptr;
            state._lockedFlagAndReaderCount = newState;
            state._waitersHead = next;
        }
        if (state._waitersHead == nullptr) {
            state._waitersTailNext = &state._waitersHead;
        }
    } else {
        state._lockedFlagAndReaderCount = kUnlocked;
    }
    return head;
}

inline void SharedMutex::resumeWaiters(LockAwaiterBase* awaiters) noexcept {
    while (awaiters != nullptr) {
        std::exchange(awaiters, awaiters->_nextAwaiter)->resume();
    }
}

}  // namespace coro
}  // namespace future_lite

#endif
