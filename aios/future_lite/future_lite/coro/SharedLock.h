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
#ifndef FUTURE_LITE_CORO_SHARED_LOCK_H
#define FUTURE_LITE_CORO_SHARED_LOCK_H

#include <memory>
#include <mutex>
#include <utility>

namespace future_lite {
namespace coro {

/// This type mirrors the interface of std::shared_lock as much as possible.
///
/// The main difference between this type and std::shared_lock is that this
/// type is designed to be used with asynchronous shared-mutex types where
/// the lock acquisition is an asynchronous operation.
///
/// TODO: Actually implement the .coLock() method on this class.
///
/// Workaround for now is to use:
///   SharedLock<SharedMutex> lock{mutex, std::defer_lock};
///   ...
///   lock = co_await lock.mutex()->coScopedLockShared();
template <typename Mutex>
class SharedLock {
public:
    SharedLock() noexcept : _mutex(nullptr), _locked(false) {}

    explicit SharedLock(Mutex& mutex, std::defer_lock_t) noexcept
        : _mutex(std::addressof(mutex)), _locked(false) {}

    explicit SharedLock(Mutex& mutex, std::adopt_lock_t) noexcept
        : _mutex(std::addressof(mutex)), _locked(true) {}

    explicit SharedLock(Mutex& mutex, std::try_to_lock_t) noexcept(
        noexcept(mutex.tryLockShared()))
        : _mutex(std::addressof(mutex)), _locked(mutex.tryLockShared()) {}

    SharedLock(SharedLock&& other) noexcept
        : _mutex(std::exchange(other._mutex, nullptr)),
          _locked(std::exchange(other._locked, false)) {}

    SharedLock(const SharedLock&) = delete;
    SharedLock& operator=(const SharedLock&) = delete;

    ~SharedLock() {
        if (_locked) {
            _mutex->unlockShared();
        }
    }

    SharedLock& operator=(SharedLock&& other) noexcept {
        SharedLock temp(std::move(other));
        swap(temp);
        return *this;
    }

    Mutex* mutex() const noexcept {
        return _mutex;
    }

    Mutex* release() noexcept {
        _locked = false;
        return std::exchange(_mutex, nullptr);
    }

    bool ownsLock() const noexcept {
        return _locked;
    }

    explicit operator bool() const noexcept {
        return ownsLock();
    }

    bool tryLock() noexcept(noexcept(_mutex->tryLockShared())) {
        assert(!_locked);
        assert(_mutex != nullptr);
        _locked = _mutex->tryLockShared();
        return _locked;
    }

    void unlock() noexcept(noexcept(_mutex->unlockShared())) {
        assert(_locked);
        _locked = false;
        _mutex->unlockShared();
    }

    void swap(SharedLock& other) noexcept {
        std::swap(_mutex, other._mutex);
        std::swap(_locked, other._locked);
    }

private:
    Mutex* _mutex;
    bool _locked;
};

}  // namespace coro
}  // namespace future_lite

#endif
