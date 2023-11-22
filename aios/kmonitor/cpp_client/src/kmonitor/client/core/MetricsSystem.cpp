/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 10:06
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#include "kmonitor/client/core/MetricsSystem.h"

#include <chrono>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "kmonitor/client/KMonitor.h"
#include "kmonitor/client/KMonitorFactory.h"
#include "kmonitor/client/core/MetricsConfig.h"
#include "kmonitor/client/core/MetricsRecord.h"
#include "kmonitor/client/core/MetricsSource.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "kmonitor/client/net/BatchFlumeEvent.h"
#include "kmonitor/client/sink/FlumeSink.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);
AUTIL_LOG_SETUP(kmonitor, MetricsSystem);

using std::map;
using std::set;
using std::string;
using std::vector;

MetricsSystem::MetricsSystem() {
    timer_interval_us_ = 100 * 1000;
    period_us_ = 1 * 1000 * 1000;
    last_trigger_ = 0;
    started_ = false;

    auto seed = std::chrono::system_clock::now().time_since_epoch().count();
    // Generate random numbers using the Mersenne Twister algorithm
    std::mt19937 gen(seed);
    // Generate a random integer between 0-MAX_LATENCY seconds with a precision of us
    std::uniform_int_distribution<> dist(0, MAX_LATENCY * 1000 * 1000);
    random_time_ = dist(gen);
}

MetricsSystem::~MetricsSystem() {
    timer_thread_ptr_.reset();
    send_thread_ptr_.reset();
}

void MetricsSystem::Init(MetricsConfig *config) {
    if (!initSink(config)) {
        AUTIL_LOG(ERROR, "sink init fail, metric system event timer stop");
        return;
    }
    if (config->manually_mode()) {
        AUTIL_LOG(WARN, "manually mode enabled, skip start timer");
    } else {
        StartTimer();
        AUTIL_LOG(INFO, "timer started");
    }
    started_ = true;
}

bool MetricsSystem::initSink(MetricsConfig *config) {
    auto sink =
        std::make_shared<FlumeSink>(config->tenant_name(), config->sink_address(), config->enable_log_file_sink());
    if (!sink->Init()) {
        AUTIL_LOG(ERROR, "Add and init sink %s failed", sink->GetName().c_str());
        return false;
    }
    return AddSink(sink);
}

bool MetricsSystem::Started() { return started_; }

void MetricsSystem::AddSource(MetricsSource *source) {
    autil::ScopedWriteLock lock(source_lock_);
    source_map_.insert(make_pair(source->Name(), source));
    AUTIL_LOG(INFO, "source size [%ld] ,add name[%s]", source_map_.size(), source->Name().c_str());
}

void MetricsSystem::DelSource(const string &name) {
    autil::ScopedWriteLock lock(source_lock_);
    source_map_.erase(name);
    AUTIL_LOG(INFO, "remove source name [%s], current source size [%ld]", name.c_str(), source_map_.size());
}

void MetricsSystem::Stop() {
    if (timer_thread_ptr_) {
        timer_thread_ptr_->stop();
    }
    if (send_thread_ptr_) {
        send_thread_ptr_->stop();
    }
    started_ = false;
    AUTIL_LOG(INFO, "metrics system stoped");
}

void MetricsSystem::PublishMetrics(MetricsRecords &records) {
    for (auto e : sinks_) {
        auto &sink = e.second;
        auto &rawRecords = records.getRecords();
        for (auto record : rawRecords) {
            sink->PutMetrics(record);
        }
        sink->Flush();
        AUTIL_LOG(DEBUG, "publish metrics to %s, metrics num is %ld", sink->GetName().c_str(), records.size());
    }
}

MetricsRecords MetricsSystem::SampleMetrics(const set<MetricLevel> &levels, int64_t timeMs) {
    collector_.Clear();
    autil::ScopedReadLock lock(source_lock_);
    map<string, MetricsSource *>::iterator iter = source_map_.begin();
    for (; iter != source_map_.end(); iter++) {
        iter->second->GetMetrics(&collector_, levels, timeMs);
    }
    return collector_.StealRecords();
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
    DoSnapshot(levels, timeAlignUs);
}

void MetricsSystem::DoSnapshot(const set<MetricLevel> &levels, int64_t timeAlignUs) {
    auto records = SampleMetrics(levels, timeAlignUs / 1000);
    std::lock_guard<std::mutex> lock(queue_mutex_);
    publish_queue_.push(std::make_pair(timeAlignUs, std::move(records)));
}

void MetricsSystem::SendEvent(int64_t nowUs) {
    if (nowUs == 0) {
        nowUs = autil::TimeUtility::currentTime();
    }
    std::pair<int64_t, MetricsRecords> sampleRes;
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (publish_queue_.empty()) {
            return;
        }
        if (nowUs < publish_queue_.front().first + random_time_) {
            return;
        }
        sampleRes = std::move(publish_queue_.front());
        publish_queue_.pop();
    }
    auto &records = sampleRes.second;
    PublishMetrics(records);
}

void MetricsSystem::StartTimer() {
    timer_thread_ptr_ = autil::LoopThread::createLoopThread(
        std::bind(&MetricsSystem::OnTimerEvent, this, 0), timer_interval_us_, "KmonReport");
    if (!timer_thread_ptr_) {
        AUTIL_LOG(WARN, "create kmon sample metrics thread failed");
    }
    send_thread_ptr_ = autil::LoopThread::createLoopThread(
        std::bind(&MetricsSystem::SendEvent, this, 0), timer_interval_us_, "KmonSend");
    if (!send_thread_ptr_) {
        AUTIL_LOG(WARN, "create kmon send metrics thread failed");
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
    } else {
        return SinkPtr();
    }
}

void MetricsSystem::SetRandomTime(int64_t randomTime) { random_time_ = randomTime; }
int64_t MetricsSystem::GetRandomTime() const { return random_time_; }

void MetricsSystem::ManuallySnapshot() {
    int64_t nowUs = autil::TimeUtility::currentTime();
    AUTIL_LOG(DEBUG, "sample metrics running[%ld]....", nowUs);
    int64_t timeAlignUs = nowUs - nowUs % 1000;
    if (timeAlignUs == last_trigger_) {
        return;
    }
    last_trigger_ = timeAlignUs;
    auto levels = MetricLevelManager::GetLevel(0);
    DoSnapshot(levels, timeAlignUs);
    SendEvent(std::numeric_limits<int64_t>::max());
}

END_KMONITOR_NAMESPACE(kmonitor);
