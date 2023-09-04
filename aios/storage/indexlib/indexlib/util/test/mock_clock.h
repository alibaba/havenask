#pragma once

#include <stddef.h>
#include <time.h>
#include <unistd.h>

#include "indexlib/indexlib.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/clock.h"

namespace indexlib { namespace util {
class MockClock : public Clock
{
public:
    MockClock() {}
    virtual ~MockClock() {}

    MockClock(const MockClock&) = delete;
    MockClock& operator=(const MockClock&) = delete;
    MockClock(MockClock&&) = delete;
    MockClock& operator=(MockClock&&) = delete;

public:
    uint64_t Now() override { return _now; }
    void SleepFor(uint64_t duration_us) override { _now += duration_us; }

private:
    uint64_t _now = 0;
};
}} // namespace indexlib::util