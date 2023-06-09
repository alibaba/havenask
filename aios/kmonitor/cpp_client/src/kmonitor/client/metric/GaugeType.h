/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-03-02 10:47
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#ifndef KMONITOR_CLIENT_METRIC_GAUGETYPE_H_
#define KMONITOR_CLIENT_METRIC_GAUGETYPE_H_

#include "kmonitor/client/common/Common.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

enum GaugeType {
    MIN = 0,
    MAX = 1,
    AVG = 2,
    P99 = 3,
    P999 = 4
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif  // KMONITOR_CLIENT_METRIC_GAUGETYPE_H_
