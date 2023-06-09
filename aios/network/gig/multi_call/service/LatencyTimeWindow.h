/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2019-05-28 16:40
 * Author Name: 夕奇
 * Author Email: xiqi.lq@alibaba-inc.com
 */

#ifndef ISEARCH_MULTI_CALL_LATENCYTIMEWINDOW_H_
#define ISEARCH_MULTI_CALL_LATENCYTIMEWINDOW_H_

#include "aios/network/gig/multi_call/common/common.h"
#include "autil/AtomicCounter.h"

namespace multi_call {

class LatencyTimeWindow {
public:
    explicit LatencyTimeWindow(int64_t windowSize = 1000);
    ~LatencyTimeWindow();

private:
    LatencyTimeWindow(const LatencyTimeWindow &);
    LatencyTimeWindow &operator=(const LatencyTimeWindow &);

public:
    int64_t push(int64_t latency);
    inline int64_t getAvgLatency() { return _currentAvg.getValue() >> 10; }
    inline void setWindowSize(int64_t windowSize) {
        if (windowSize == 0)
            return;
        _windowSize = windowSize;
    }

private:
    int64_t _windowSize;
    autil::AtomicCounter _currentAvg;
};

MULTI_CALL_TYPEDEF_PTR(LatencyTimeWindow);

typedef std::map<std::string, LatencyTimeWindowPtr> LatencyTimeWindowMap;
MULTI_CALL_TYPEDEF_PTR(LatencyTimeWindowMap);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_LATENCYTIMEWINDOW_H_
