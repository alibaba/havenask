/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 10:47
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#include "kmonitor/client/metric/LogarithmicMapping.h"

#include <float.h>
#include <limits.h>
#include <math.h>

BEGIN_KMONITOR_NAMESPACE(kmonitor);
using namespace std;

LogarithmicMapping::LogarithmicMapping(double relativeAccuracy) {
    relativeAccuracy_ = 0.01;
    if (relativeAccuracy > 0 && relativeAccuracy < 1) {
        relativeAccuracy_ = relativeAccuracy;
    }

    logGamma_ = ::log((1 + relativeAccuracy_) / (1 - relativeAccuracy_));
}

int32_t LogarithmicMapping::index(double value) {
    double index = ::log(value) / logGamma_;
    return index >= 0 ? (int32_t)index : (int32_t)index - 1;
}

double LogarithmicMapping::value(int32_t index) { return ::exp(index * logGamma_) * (1 + relativeAccuracy_); }

double LogarithmicMapping::minIndexableValue() {
    double integer_min = ::exp((INT_MIN + 1) * logGamma_);
    double double_min = DBL_MIN * ::exp(logGamma_);
    if (integer_min > double_min) {
        return integer_min;
    } else {
        return double_min;
    }
}

double LogarithmicMapping::maxIndexableValue() {
    double inter_max = ::exp(INT_MAX * logGamma_);
    double double_max = DBL_MAX / (1 + relativeAccuracy_);

    if (inter_max < double_max) {
        return inter_max;
    } else {
        return double_max;
    }
}

END_KMONITOR_NAMESPACE(kmonitor);
