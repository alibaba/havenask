/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 11:32
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#ifndef KMONITOR_CLIENT_METRICTYPE_H_
#define KMONITOR_CLIENT_METRICTYPE_H_

#include "kmonitor/client/common/Common.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

enum MetricType : unsigned int {
    GAUGE,
    SUMMARY,
    COUNTER,
    QPS,
    RAW,
    STATUS
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif  // KMONITOR_CLIENT_METRICTYPE_H_
