/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 15:44
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#include "kmonitor/client/core/MetricsRecord.h"

#include <string>
#include <vector>

#include "autil/StringUtil.h"
#include "kmonitor/client/core/MetricsInfo.h"
#include "kmonitor/client/core/MetricsTags.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

using namespace std;

MetricsRecord::MetricsRecord(const MetricsInfoPtr &info, const MetricsTagsPtr &tags, int64_t timestamp)
    : info_(info), timestamp_(timestamp), tags_(tags) {}

MetricsRecord::~MetricsRecord() {
    info_.reset();
    tags_.reset();
    for (auto item : values_) {
        delete item;
    }
}

MetricsRecord::MetricsRecord(const MetricsRecord &o) : info_(o.info_), timestamp_(o.timestamp_), tags_(o.tags_) {
    for (auto &v : o.values_) {
        values_.push_back(new MetricsValue(*v));
    }
}

MetricsRecord &MetricsRecord::operator=(const MetricsRecord &o) {
    if (this == &o) {
        return *this;
    }
    info_ = o.info_;
    timestamp_ = o.timestamp_;
    tags_ = o.tags_;
    for (auto &v : o.values_) {
        values_.push_back(new MetricsValue(*v));
    }
    return *this;
}

const string &MetricsRecord::Name() const { return info_->Name(); }

void MetricsRecord::AddValue(const MetricsInfoPtr &info, double value) {
    auto str = autil::StringUtil::fToString(value, /*format*/ "%.10g");
    auto item = new MetricsValue(info, str);
    values_.push_back(item);
}

void MetricsRecord::AddValue(const MetricsInfoPtr &info, const std::string &value) {
    auto item = new MetricsValue(info, value);
    values_.push_back(item);
}

END_KMONITOR_NAMESPACE(kmonitor);
