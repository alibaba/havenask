/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 11:00
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#include "kmonitor/client/core/MetricsCollector.h"

#include <string>
#include <vector>

#include "kmonitor/client/core/MetricsRecord.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

using namespace std;

MetricsCollector::MetricsCollector() {}

MetricsCollector::~MetricsCollector() { Clear(); }

MetricsRecord *MetricsCollector::AddRecord(const string &name, const MetricsTagsPtr &tags, int64_t curTime) {
    auto info = std::make_shared<MetricsInfo>(name, name);
    MetricsRecord *record = new MetricsRecord(info, tags, curTime);
    records_.push_back(record);
    return record;
}

void MetricsCollector::Clear() { records_.clear(); }

MetricsRecords MetricsCollector::StealRecords() { return std::move(records_); }

const MetricsRecords &MetricsCollector::GetRecords() const { return records_; }

END_KMONITOR_NAMESPACE(kmonitor);
