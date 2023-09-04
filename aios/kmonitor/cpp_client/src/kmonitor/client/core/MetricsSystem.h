/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 10:06
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#ifndef KMONITOR_CLIENT_CORE_METRICSSYSTEM_H_
#define KMONITOR_CLIENT_CORE_METRICSSYSTEM_H_

#include <map>
#include <set>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "kmonitor/client/MetricLevel.h"
#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/core/MetricsCollector.h"
#include "kmonitor/client/sink/Sink.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class MetricsConfig;
class MetricsSource;
class MetricsConfig;
class MetricsRecord;

class MetricsSystem {
public:
    explicit MetricsSystem();
    ~MetricsSystem();
    MetricsSystem(const MetricsSystem &) = delete;
    MetricsSystem &operator=(const MetricsSystem &) = delete;

public:
    void Init(MetricsConfig *config);
    void Stop();
    bool Started();

    void AddSource(MetricsSource *source);
    void DelSource(const std::string &sourceName);
    bool AddSink(const SinkPtr &sink);
    SinkPtr GetSink(const std::string &name) const;

private:
    bool initSink(MetricsConfig *config);
    const MetricsRecords &SampleMetrics(const std::set<MetricLevel> &levels, int64_t now);
    void PublishMetrics(const MetricsRecords &records);
    void StartTimer();
    void OnTimerEvent(int64_t nowUs);

private:
    int timer_interval_us_;
    int period_us_;
    int64_t last_trigger_;
    std::string prefix_;
    bool started_;
    MetricsCollector collector_;
    mutable autil::ReadWriteLock source_lock_;
    std::map<std::string, MetricsSource *> source_map_;
    std::map<std::string, SinkPtr> sinks_;
    autil::LoopThreadPtr timer_thread_ptr_;

private:
    AUTIL_LOG_DECLARE();
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_CORE_METRICSSYSTEM_H_
