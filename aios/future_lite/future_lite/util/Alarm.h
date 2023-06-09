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

#include <functional>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "absl/synchronization/notification.h"
#include "absl/time/time.h"

namespace future_lite {

namespace util {

// Allows execution of handlers at a specified time in the future
// Guarantees:
//  - All handlers are executed ONCE, even if cancelled
//  - If Alarm is destroyed, it will cancel all handlers.
//  - Handlers are ALWAYS executed in the Alarm worker thread.
//  - Handlers execution order is NOT guaranteed
//
// NOTE: for performance consideration, user should only put the time
// critical work in the timer handler like scheduling a long-run computing
// work in a background thread pool to execute.
class Alarm {
public:
    explicit Alarm(const std::string &name);
    Alarm(const Alarm &) = delete;
    Alarm &operator=(const Alarm &) = delete;

    ~Alarm() {
        Shutdown();
    }

    typedef uint64_t Handle;
    static constexpr Alarm::Handle kInvalidHandle = 0;

    struct TimerResult {
        // Indicates if the expired timer needs to be reschedule.
        // NOTE: this enables user implementing a recurring timer.
        bool reschedule = false;
        // If 'reschedule' is true, the next timer delay.
        absl::Duration delay = absl::ZeroDuration();
    };
    typedef std::function<TimerResult(bool)> TimerFn;
    // Adds a new timer with callback. The function returns a timer
    // handle which can be used to cancel a pending timer.
    // NOTE: the timer resolution is millisecond.
    Handle Add(absl::Duration delay, TimerFn callback);

    // Cancels the specified timer. The function returns true if the timer was
    // cancelled. Otherwise, returns false if it was too late to cancel or
    // 'timerHandle' is invalid.
    bool Cancel(Handle timerHandle);

    // Cancels all timers. The function returns the number of timers cancelled.
    size_t CancelAll();

    // Return the number of pending timers for testing.
    size_t TEST_NumOfPendingTimers() const {
        absl::ReaderMutexLock l(&_lock);
        return _timers.size();
    }
    // Wait for the number of pending timers to reach to 'expectNumTimers'.
    void TEST_WaitForNumOfPendingTimers(int expectNumTimers);

private:
    struct TimerEntry {
        // Timer expiring time point.
        absl::Time expiringTime;
        // Timer duration.
        absl::Duration delay;
        // Internal timer id. If it is set 0 means it was cancelled
        Handle id;
        TimerFn handler;
        bool operator>(const TimerEntry &other) const {
            return expiringTime > other.expiringTime;
        }
    };
    // Organize all the pending timers in a min priority_queue sorted by their
    // expiring time.
    class Queue : public std::priority_queue<TimerEntry, std::vector<TimerEntry>, std::greater<TimerEntry>> {
    public:
        std::vector<TimerEntry> &GetAllTimers() {
            return this->c;
        }
    };

    // Timer worker thread main function.
    void Run() ABSL_LOCKS_EXCLUDED(_lock);
    bool GetNextTimerExpiringTime(absl::Time *expiringTime) ABSL_EXCLUSIVE_LOCKS_REQUIRED(_lock);
    void CheckTimers() ABSL_EXCLUSIVE_LOCKS_REQUIRED(_lock);
    // Shutdown Alarm and cancell all timers.
    void Shutdown() ABSL_LOCKS_EXCLUDED(_lock);
    // Return the name of this alarm.
    std::string name() {
        return _name;
    }

    // The name of this alarm which is also used to set the name of the internal
    // timer thread.
    const std::string _name;
    mutable absl::Mutex _lock;
    bool _finish ABSL_GUARDED_BY(_lock) = false;
    bool _cancel ABSL_GUARDED_BY(_lock) = false;
    Queue _timers ABSL_GUARDED_BY(_lock);
    uint64_t _nextHandle ABSL_GUARDED_BY(_lock) = 0;
    std::thread _worker;
};

} // namespace util
} // namespace future_lite
