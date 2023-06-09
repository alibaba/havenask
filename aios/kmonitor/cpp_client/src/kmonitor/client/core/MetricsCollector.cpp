/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 11:00
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#include <string>
#include <vector>
#include "kmonitor/client/core/MetricsRecord.h"
#include "kmonitor/client/core/MetricsCollector.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

using namespace std;

MetricsCollector::MetricsCollector() {
}

MetricsCollector::~MetricsCollector() {
    Clear();
}

MetricsRecord *MetricsCollector::AddRecord(
        const string &name,
        const MetricsTagsPtr &tags,
        int64_t curTime) {
    auto info = std::make_shared<MetricsInfo>(name, name);
    MetricsRecord *record = new MetricsRecord(info, tags, curTime);
    records_.push_back(record);
    return record;
}

void MetricsCollector::Clear() {
    for (size_t i = 0; i < records_.size(); ++i) {
        delete records_[i];
    }
    records_.clear();
}

const MetricsRecords &MetricsCollector::GetRecords() const {
    return records_;
}



END_KMONITOR_NAMESPACE(kmonitor);

