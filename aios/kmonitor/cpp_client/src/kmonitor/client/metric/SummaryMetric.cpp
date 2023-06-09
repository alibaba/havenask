/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 10:47
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#include <string>
#include "kmonitor/client/metric/SummaryMetric.h"
#include "kmonitor/client/core/MetricsInfo.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

using namespace std;

const double SummaryMetric::RELATIVE_ACCURACY = 0.01;

SummaryMetric::SummaryMetric(const string &name)
    : Metric(name) {
    const string fullMetric = name + ".summary";
    summary_info_ = MetricsInfoPtr(new MetricsInfo(fullMetric, fullMetric, {{Metric::HEADER_FORMAT, "ddsketch"}}));
    ddsketch_ = new DDSketch(RELATIVE_ACCURACY);
}

void SummaryMetric::doUpdate(double value) {
    if (::isnan(value)) {
        return;
    }
    calculator_.Add(value);
    ddsketch_->accept(value);
}

void SummaryMetric::doSnapshot(MetricsRecord *record, int64_t period) {
    if (ddsketch_ == nullptr) {
        ddsketch_ = new DDSketch(RELATIVE_ACCURACY);
        return;
    }
    record->AddValue(info_, calculator_.ToString());
    calculator_.Reset();
    ddsketch_->Flush();
    record->AddValue(summary_info_, autil::legacy::FastToJsonString(ddsketch_, false));
    delete ddsketch_;
    ddsketch_ = new DDSketch(RELATIVE_ACCURACY);
}

SummaryMetric::~SummaryMetric() {
    summary_info_.reset();
    delete ddsketch_;
}

END_KMONITOR_NAMESPACE(kmonitor);

