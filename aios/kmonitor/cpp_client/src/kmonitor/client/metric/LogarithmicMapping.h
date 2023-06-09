/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 10:47
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#ifndef KMONITOR_CLIENT_METRIC_LOGARITHMICMAPPING_H_
#define KMONITOR_CLIENT_METRIC_LOGARITHMICMAPPING_H_

#include <string>
#include "kmonitor/client/common/Common.h"
#include <math.h>

BEGIN_KMONITOR_NAMESPACE(kmonitor);
static const double EPSINON = 0.000001;

class LogarithmicMapping
{
public:
    LogarithmicMapping(double relativeAccuracy = 0.01);
    virtual ~LogarithmicMapping() {}
    int32_t index(double value);
    
    double value(int32_t index);
    
    double relativeAccuracy() {
        return relativeAccuracy_;
    }
    
    double minIndexableValue();
    
    double maxIndexableValue();

    friend bool operator==(LogarithmicMapping& lMap, LogarithmicMapping& rMap) {
        if (&lMap == &rMap) {
            return true;
        }
        return (lMap.logGamma_ - rMap.logGamma_) < EPSINON && (lMap.logGamma_ - rMap.logGamma_) > -EPSINON;
    }

private:
    double relativeAccuracy_;
    double logGamma_;
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif  // KMONITOR_CLIENT_METRIC_LOGARITHMICMAPPING_H_
