/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 10:47
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#include "kmonitor/client/metric/RawMetric.h"

#include <string>
#include <vector>

BEGIN_KMONITOR_NAMESPACE(kmonitor);

using namespace std;
// IGRAPH_LOG_SETUP(client, RawMetric);

RawMetric::RawMetric(const string &name) : Metric(name) {}

void RawMetric::doUpdate(double value) { values_.push_back(value); }

void RawMetric::doSnapshot(MetricsRecord *record, int64_t period) {
    for (vector<double>::iterator iter = values_.begin(); iter != values_.end(); ++iter) {
        record->AddValue(info_, *iter);
    }
    values_.clear();
}

RawMetric::~RawMetric() { values_.clear(); }

END_KMONITOR_NAMESPACE(kmonitor);
