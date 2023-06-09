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

#include <stdint.h>


namespace autil {

typedef uint64_t TimePoint;
typedef uint64_t Duration;  // us

class TimeSpan {
 public:
  // Set $start_ = now
  TimeSpan();

  // Just an inner time point to calculate duration, not a real timestamp.
  static TimePoint GetCurrentTimePoint();

  // Get start time point, not a real timestamp
  TimePoint GetStart() const { return start_; }

  // Get duration from $start to now.
  Duration GetDuration() const;

  // Get duration from $start_ to $end.
  // Note that if $end < $start_, 0 will be returned.
  Duration GetDuration(TimePoint end) const;

  // Return duration from $start_ to now, and set $start_ = now
  Duration Rotate();

  // Return duration from $start_ to $end, and set $start_ = $end
  Duration Rotate(TimePoint end);

 private:
  TimePoint start_;
};

class Deadline {
 public:
  // Set a deadline after $duration us.
  explicit Deadline(Duration duration);

  // Set a deadline after $start $duration us.
  Deadline(TimePoint start, Duration duration);

  static TimePoint GetDeadline(TimePoint start, Duration duration);

  // Return true if the deadline is reached.
  operator bool() const;

  // Return true if the deadline is after now.
  bool After(TimePoint now) const;

 private:
  TimePoint deadline_;
  mutable bool reached_ = false;
};

}

