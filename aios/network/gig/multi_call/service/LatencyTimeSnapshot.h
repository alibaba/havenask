/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2019-05-28 17:52
 * Author Name: 夕奇
 * Author Email: xiqi.lq@alibaba-inc.com
 */

#ifndef ISEARCH_MULTI_CALL_LATENCYTIMESNAPSHOT_H_
#define ISEARCH_MULTI_CALL_LATENCYTIMESNAPSHOT_H_

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/service/LatencyTimeWindow.h"
#include "autil/Lock.h"

namespace multi_call {

class LatencyTimeSnapshot {
public:
    LatencyTimeSnapshot();
    ~LatencyTimeSnapshot();

private:
    LatencyTimeSnapshot(const LatencyTimeSnapshot &);
    LatencyTimeSnapshot &operator=(const LatencyTimeSnapshot &);

public:
    void updateLatencyTimeWindow(const std::string &bizName,
                                 int64_t windowSize);
    LatencyTimeWindowPtr getLatencyTimeWindow(const std::string &bizName);
    int64_t getAvgLatency(const std::string &bizName);
    int64_t pushLatency(const std::string &bizName, int64_t latency);

private:
    autil::ReadWriteLock _mapLock;
    LatencyTimeWindowMapPtr _latencyMap;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(LatencyTimeSnapshot);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_LATENCYTIMESNAPSHOT_H_
