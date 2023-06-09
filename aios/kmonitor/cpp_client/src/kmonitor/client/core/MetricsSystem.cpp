/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 10:06
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#include <set>
#include <map>
#include <string>
#include <vector>
#include "autil/Log.h"
#include "kmonitor/client/sink/FlumeSink.h"
#include "kmonitor/client/KMonitorFactory.h"
#include "kmonitor/client/KMonitor.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "kmonitor/client/core/MetricsRecord.h"
#include "kmonitor/client/core/MetricsSource.h"
#include "kmonitor/client/core/MetricsConfig.h"
#include "kmonitor/client/core/MetricsSystem.h"
#include "kmonitor/client/net/BatchFlumeEvent.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);
AUTIL_LOG_SETUP(kmonitor, MetricsSystem);

using std::set;
using std::map;
using std::string;
using std::vector;

MetricsSystem::MetricsSystem() {
    timer_interval_us_ = 100 * 1000;
    period_us_ = 1 * 1000 * 1000;
    last_trigger_ = 0;
    started_ = false;
}

MetricsSystem::~MetricsSystem() {
    timer_thread_ptr_.reset();
}

void MetricsSystem::Init(MetricsConfig *config) {
    if (!initSink(config)) {
        AUTIL_LOG(ERROR, "sink init fail, metric system event timer stop");
        return;
    }
    StartTimer();
    started_ = true;
}

bool MetricsSystem::initSink(MetricsConfig *config) {
    auto sink = std::make_shared<FlumeSink>(config->tenant_name(),
                                            config->sink_address(),
                                            config->remote_sink_address(),
                                            config->enable_log_file_sink());
    if (!sink->Init()) {
        AUTIL_LOG(ERROR, "Add and init sink %s failed",
                     sink->GetName().c_str());
        return false;
    }
    return AddSink(sink);
}

bool MetricsSystem::Started() {
    return started_;
}

void MetricsSystem::AddSource(MetricsSource *source) {
    autil::ScopedWriteLock lock(source_lock_);
    source_map_.insert(make_pair(source->Name(), source));
    AUTIL_LOG(INFO, "source size [%ld] ,add name[%s]", source_map_.size(), source->Name().c_str());
}

void MetricsSystem::DelSource(const string &name) {
    autil::ScopedWriteLock lock(source_lock_);
    source_map_.erase(name);
    AUTIL_LOG(INFO, "remove source name [%s], current source size [%ld]",
                 name.c_str(), source_map_.size());
}

void MetricsSystem::Stop() {
    if (timer_thread_ptr_) {
        timer_thread_ptr_->stop();
    }
    started_ = false;
    AUTIL_LOG(INFO, "metrics system stoped");
}

void MetricsSystem::PublishMetrics(const MetricsRecords &records) {
    for (auto e : sinks_) {
        auto &sink = e.second;
        for (auto record : records) {
            sink->PutMetrics(record);
        }
        sink->Flush();
        AUTIL_LOG(DEBUG, "publish metrics to %s, metrics num is %ld",
                     sink->GetName().c_str(),
                     records.size());
    }
}

const MetricsRecords &MetricsSystem::SampleMetrics(const set<MetricLevel>& levels, int64_t timeMs) {
    collector_.Clear();
    autil::ScopedReadLock lock(source_lock_);
    map<string, MetricsSource*>::iterator iter = source_map_.begin();
    for (; iter != source_map_.end(); iter++) {
        iter->second->GetMetrics(&collector_, levels, timeMs);
    }
    return collector_.GetRecords();
}

void MetricsSystem::OnTimerEvent(int64_t nowUs) {
    if (nowUs == 0) {
        nowUs = autil::TimeUtility::currentTime();
    }
    AUTIL_LOG(DEBUG, "sample metrics running[%ld]....", nowUs);
    int64_t timeAlignUs = nowUs - nowUs % period_us_;
    if (timeAlignUs == last_trigger_) {
        return;
    }
    last_trigger_ = timeAlignUs;
    set<MetricLevel> levels = MetricLevelManager::GetLevel(timeAlignUs / 1000 / 1000);
    AUTIL_LOG(DEBUG, "metric level size:%lu", levels.size());
    // default metric level is NORMAL, it will report for 10S/period
    auto sampleRes = SampleMetrics(levels, timeAlignUs / 1000);
    PublishMetrics(sampleRes);
}

void MetricsSystem::StartTimer() {
    timer_thread_ptr_ = autil::LoopThread::createLoopThread(
        std::bind(&MetricsSystem::OnTimerEvent, this, 0), timer_interval_us_, "KmonReport");
    if (!timer_thread_ptr_) {
        AUTIL_LOG(WARN, "create kmon sample metrics thread failed");
    }
    AUTIL_LOG(INFO, "sample metrics thread started");
}

bool MetricsSystem::AddSink(const SinkPtr &sink) {
    auto &name = sink->GetName();
    sinks_[name] = sink;
    return true;
}

SinkPtr MetricsSystem::GetSink(const std::string &name) const {
    auto iter = sinks_.find(name);
    if (iter != sinks_.end()) {
        return iter->second;
    }
    else {
        return SinkPtr();
    }
}

END_KMONITOR_NAMESPACE(kmonitor);
