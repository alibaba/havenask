/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 14:32
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#include "kmonitor/client/metric/Metric.h"

#include "kmonitor/client/core/MetricsInfo.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

const std::string Metric::HEADER_IP("ip");
const std::string Metric::HEADER_TENANT("tenant");
const std::string Metric::HEADER_FORMAT("format");

Metric::Metric(const std::string &name) : untouch_num_(-1), ref_cnt_(0) {
    info_ = MetricsInfoPtr(new MetricsInfo(name, name));
}

Metric::~Metric() {}

END_KMONITOR_NAMESPACE(kmonitor);
