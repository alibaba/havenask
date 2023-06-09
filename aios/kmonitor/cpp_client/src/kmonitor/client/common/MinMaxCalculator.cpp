/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-07-26 17:33
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#include "kmonitor/client/common/MinMaxCalculator.h"

#include <limits>

BEGIN_KMONITOR_NAMESPACE(kmonitor);

MinMaxCalculator::MinMaxCalculator() : count_(0), sum_(0) {
    max_ = std::numeric_limits<double>::lowest();
    min_ = std::numeric_limits<double>::max();
}

MinMaxCalculator::~MinMaxCalculator() {}

void MinMaxCalculator::Add(double value) {
    if (value > max_) {
        max_ = value;
    }
    if (value < min_) {
        min_ = value;
    }
    sum_ += value;
    count_++;
}

void MinMaxCalculator::Reset() {
    max_ = std::numeric_limits<double>::lowest();
    min_ = std::numeric_limits<double>::max();
    count_ = 0;
    sum_ = 0.0;
}

std::string MinMaxCalculator::ToString() const {
    char buf[100];
    sprintf(buf, "%.10g_%.10g_%.10g_%.10g_%d", sum_ / count_, sum_, max_, min_, count_);
    return std::string(buf);
}

END_KMONITOR_NAMESPACE(kmonitor);
