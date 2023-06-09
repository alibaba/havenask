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
#include "autil/TimeSpan.h"
#include "autil/Libdivide.h"

#include <stdint.h>
#include <stdio.h>

namespace autil {

inline uint64_t GetCycles() {
#if __x86_64__
  uint32_t low, high;
  uint64_t val;
  __asm__ __volatile__("rdtsc" : "=a"(low), "=d"(high));
  val = high;
  val = (val << 32) | low;
  return val;
#elif __aarch64__
  uint64_t virtual_timer_value;
  asm volatile("mrs %0, cntvct_el0" : "=r"(virtual_timer_value));
  return virtual_timer_value;
#else
  #error arch unsupported
#endif
}

inline double GetCpuMhz(void) {
  FILE *f;
  char buf[256];
  double mhz = 0.0;

#if __aarch64__
  f = fopen("/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_cur_freq", "r");
  if (!f) return 0.0;
  while (fgets(buf, sizeof(buf), f)) {
    uint32_t m;
    int rc;
    rc = sscanf(buf, "%u", &m);
    if (rc == 1 && mhz == 0.0) {
      mhz = (double)(m/1000);
      break;
    }
  }
#else
  f = fopen("/proc/cpuinfo", "r");
  if (!f) return 0.0;
  while (fgets(buf, sizeof(buf), f)) {
    double m;
    int rc;
    rc = sscanf(buf, "cpu MHz : %lf", &m);
    if (rc == 1 && mhz == 0.0) {
      mhz = m;
      break;
    }
  }
#endif
  fclose(f);
  return mhz;
}

static const auto kCpuMhz = GetCpuMhz();
static const libdivide::divider<uint64_t> kCpuMhzDivider(kCpuMhz);

TimeSpan::TimeSpan() : start_(GetCycles()) {
}

TimePoint TimeSpan::GetCurrentTimePoint() {
  return GetCycles();
}

Duration TimeSpan::GetDuration() const {
  return GetDuration(GetCycles());
}

Duration TimeSpan::GetDuration(TimePoint end) const {
  return end > start_ ? (end - start_) / kCpuMhzDivider : 0;
}

Duration TimeSpan::Rotate() {
  return Rotate(GetCycles());
}

Duration TimeSpan::Rotate(TimePoint end) {
  auto duration = GetDuration(end);
  start_ = end;
  return duration;
}

TimePoint Deadline::GetDeadline(TimePoint start, Duration duration) {
  return start + duration * kCpuMhz;
}

Deadline::Deadline(Duration duration) : Deadline(GetCycles(), duration) {
}

Deadline::Deadline(TimePoint start, Duration duration) : deadline_(GetDeadline(start, duration)) {
}

Deadline::operator bool() const {
  return reached_ || (reached_ = (GetCycles() >= deadline_));
}

bool Deadline::After(TimePoint now) const {
  return now < deadline_;
}

}
