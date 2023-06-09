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

#include "future_lite/util/Alarm.h"

#include <pthread.h>
#include <utility>

#include "absl/base/macros.h"
#include "alog/Logger.h"
#include "autil/Log.h"

namespace future_lite {

namespace util {
AUTIL_DECLARE_AND_SETUP_LOGGER(future_lite::util, Alarm);

Alarm::Alarm(const std::string &name)
    : _name(name)
    , _worker(&Alarm::Run, this) {
    const int ret = ::pthread_setname_np(_worker.native_handle(), name.c_str());
    if (ret != 0) {
        AUTIL_LOG(ERROR, "Set thread name %s with error: %d", name.c_str(), ret);
    }
}

void Alarm::Run() {
    absl::MutexLock l(&_lock);
    while (!_finish) {
        absl::Time nextExpiringTime;
        const bool hasPendingTimer = GetNextTimerExpiringTime(&nextExpiringTime);
        const size_t numOfTimers = _timers.size();
        auto check = [this, numOfTimers]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(_lock) -> bool {
            if (numOfTimers > 0) {
                return _cancel || numOfTimers != _timers.size();
            } else {
                return _finish || !_timers.empty();
            }
        };
        absl::Condition cond(&check);
        if (hasPendingTimer) {
            // Timers found, so wait until the next pending timer expires.
            _lock.AwaitWithDeadline(cond, nextExpiringTime);
        } else {
            // No pending times so waiting forever for anything changed.
            _lock.Await(cond);
        }

        // Check and execute all expired timers.
        CheckTimers();
    }

    // If we are shutting down, we should not have any timers left,
    // since the shutdown cancels all timers.
    if (!_timers.empty()) {
        AUTIL_LOG(ERROR, "timers is not empty, size is %lu", _timers.size());
    }
}

void Alarm::Shutdown() {
    {
        absl::MutexLock l(&_lock);
        auto check = [this]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(_lock) -> bool { return _timers.empty(); };
        absl::Condition cond(&check);
        _lock.Await(cond);
        if (_finish) {
            AUTIL_LOG(ERROR, "Alarm is finished unexpected.");
        }
        _finish = true;
    }
    _worker.join();
}

Alarm::Handle Alarm::Add(absl::Duration delay, TimerFn callback) {
    TimerEntry timer;
    const absl::Time now = absl::Now();
    timer.delay = absl::Microseconds(absl::ToInt64Microseconds(delay));
    timer.expiringTime = now + timer.delay;
    timer.handler = std::move(callback);
    absl::MutexLock l(&_lock);
    if (_cancel || _finish) {
        return kInvalidHandle;
    }
    // NOTE: timer handle starts from 1 and 0 indicates an invalid timer handle.
    Handle handle = ++_nextHandle;
    timer.id = handle;

    _timers.push(std::move(timer));
    return handle;
}

bool Alarm::Cancel(Handle timerHandle) {
    // Instead of removing the cancelled timer from the heap, we mark it
    // cancelled and insert a new one with immediate expiring time to
    // trigger cancellation callback. This ensures the timer callback will
    // be exactly executed once.
    absl::MutexLock l(&_lock);
    for (auto &timer : _timers.GetAllTimers()) {
        if (timer.id != timerHandle || timer.handler == nullptr) {
            continue;
        }
        TimerEntry newTimer;
        // Set the expiring time to pass to trigger the immediate execution
        newTimer.expiringTime = absl::InfinitePast();
        // Indicates it is a canceled timer
        newTimer.id = kInvalidHandle;
        // Move the callback from the existing timer to the new one.
        newTimer.handler = std::move(timer.handler);
        timer.handler = nullptr;

        _timers.push(std::move(newTimer));
        return true;
    }
    return false;
}

size_t Alarm::CancelAll() {
    // Setting all "end" to 0 for immediate execution while maintaining the heap
    // integrity.
    absl::MutexLock l(&_lock);
    _cancel = true;
    for (auto &timer : _timers.GetAllTimers()) {
        if (timer.id != kInvalidHandle && timer.handler != nullptr) {
            timer.expiringTime = absl::InfinitePast();
            timer.id = kInvalidHandle;
        }
    }
    return _timers.size();
}

bool Alarm::GetNextTimerExpiringTime(absl::Time *expiringTime) {
    while (!_timers.empty()) {
        TimerEntry timer(_timers.top());
        if (timer.handler != nullptr) {
            *expiringTime = timer.expiringTime;
            return true;
        }
        // Discard the cancelled timer.
        _timers.pop();
    }
    // No timers found.
    return false;
}

void Alarm::CheckTimers() {
    while (!_timers.empty() && _timers.top().expiringTime <= absl::Now()) {
        TimerEntry timer(_timers.top());
        _timers.pop();

        if (timer.handler == nullptr) {
            continue;
        }
        _lock.Unlock();
        // Execute timer callback.
        TimerResult result = timer.handler(/*is_cancelled=*/timer.id == kInvalidHandle);
        _lock.Lock();
        // If alarm is not cancelled, check if we need to reschedule this
        // timer.
        if (_cancel || !result.reschedule) {
            continue;
        }
        timer.delay = absl::Milliseconds(absl::ToInt64Milliseconds(result.delay));
        timer.expiringTime = absl::Now() + timer.delay;

        _timers.push(std::move(timer));
    }
}

void Alarm::TEST_WaitForNumOfPendingTimers(int expectNumTimers) {
    absl::MutexLock l(&_lock);
    auto check = [this, expectNumTimers]()
                     ABSL_EXCLUSIVE_LOCKS_REQUIRED(_lock) -> bool { return _timers.size() == expectNumTimers; };
    absl::Condition cond(&check);
    _lock.Await(cond);
}

} // namespace util
} // namespace future_lite
