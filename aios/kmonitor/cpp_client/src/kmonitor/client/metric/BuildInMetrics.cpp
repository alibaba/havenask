/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 10:47
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#include <string>
#include "autil/TimeUtility.h"
#include "kmonitor/client/core/MetricsInfo.h"
#include "kmonitor/client/core/MetricsRecord.h"
#include "kmonitor/client/metric/BuildInMetrics.h"
#include "kmonitor/client/common/Util.h"


BEGIN_KMONITOR_NAMESPACE(kmonitor);

BuildInMetrics::BuildInMetrics(const std::string &metricName,
                               const MetricsTagsPtr& metricsTags,
                               MetricLevel level) :
        metricName_(metricName),
        level_(level)
{
    metricsTags_ = metricsTags;
    std::string cpuName = metricName_ + "." + "proc_cpu";
    cpuInfo_ = MetricsInfoPtr(new MetricsInfo(cpuName, cpuName));

    std::string memSizeName = metricName_ + "." + "proc_mem_size";
    std::string memRssName = metricName_ + "." + "proc_mem_rss";
    std::string memRssRatioName = metricName_ + "." + "proc_mem_rss_ratio";
    std::string memUsedName = metricName_ + "." + "proc_mem_used";
    std::string memRatioName = metricName_ + "." + "proc_mem_used_ratio";
    memSizeInfo_ = MetricsInfoPtr(new MetricsInfo(memSizeName));
    memRssInfo_ = MetricsInfoPtr(new MetricsInfo(memRssName));
    memRssRatioInfo_ = MetricsInfoPtr(new MetricsInfo(memRssRatioName));
    memUsedInfo_ = MetricsInfoPtr(new MetricsInfo(memUsedName));
    memUsedRatioInfo_ = MetricsInfoPtr(new MetricsInfo(memRatioName));
}

BuildInMetrics::~BuildInMetrics()
{}

void BuildInMetrics::Snapshot(MetricsCollector *collector, int64_t curTimeMs,
                              const std::set<MetricLevel> *levels) {
    if (levels != NULL && levels->find(level_) == levels->end()) {
        return;
    }

    // time align, 默认传进来的是ms
    curTimeMs = Util::TimeAlign(curTimeMs, (int64_t)MetricLevelManager::GetLevelPeriod(level_) * 1000);

    getProcCpu(collector, curTimeMs);
    getProcMem(collector, curTimeMs);
}

void BuildInMetrics::getProcCpu(MetricsCollector *collector, int64_t curTime) {
    double usage = processCpu_.getUsage();
    if (usage >= 100.0) {
        return;
    }
    MetricsRecord *record = collector->AddRecord(cpuInfo_->Name(), metricsTags_, curTime);
    if (record == NULL) {
        return;
    }
    record->AddValue(cpuInfo_, usage);
}

void BuildInMetrics::getProcMem(MetricsCollector *collector, int64_t curTime) {
    MetricsRecord *record = NULL;
    procMem_.update();
    record = collector->AddRecord(memSizeInfo_->Name(), metricsTags_, curTime);
    if (record != NULL) {
        record->AddValue(memSizeInfo_, procMem_.getMemSize());
    }

    record = collector->AddRecord(memRssInfo_->Name(), metricsTags_, curTime);
    if (record != NULL) {
        record->AddValue(memRssInfo_, procMem_.getMemRss());
    }

    record = collector->AddRecord(memRssRatioInfo_->Name(), metricsTags_, curTime);
    if (record != NULL) {
        record->AddValue(memRssRatioInfo_, procMem_.getMemRssRatio());
    }

    record = collector->AddRecord(memUsedInfo_->Name(), metricsTags_, curTime);
    if (record != NULL) {
        record->AddValue(memUsedInfo_, procMem_.getMemUsed());
    }

    record = collector->AddRecord(memUsedRatioInfo_->Name(), metricsTags_, curTime);
    if (record != NULL) {
        record->AddValue(memUsedRatioInfo_, procMem_.getMemUsedRatio());
    }
}

END_KMONITOR_NAMESPACE(kmonitor);
