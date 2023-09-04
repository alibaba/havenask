#pragma once

#include "iquan/common/Common.h"

namespace iquan {

class Counter {
public:
    Counter()
        : _count(0)
        , _elapsed(0) {}

    void reset() {
        _count = 0;
        _elapsed = 0;
    }

    int64_t count() {
        return _count;
    }

    void addCount(int64_t count) {
        _count += count;
    }

    int64_t elapsed() {
        return _elapsed;
    }

    void addElapsed(int64_t elapsed) {
        _elapsed += elapsed;
    }

private:
    int64_t _count;
    int64_t _elapsed;
};

} // namespace iquan
