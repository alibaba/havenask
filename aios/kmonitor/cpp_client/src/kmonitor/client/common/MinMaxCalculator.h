/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-07-26 17:33
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#ifndef KMONITOR_CLIENT_COMMON_MINMAXCALCULATOR_H_
#define KMONITOR_CLIENT_COMMON_MINMAXCALCULATOR_H_

#include <limits>

#include "kmonitor/client/common/Common.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class MinMaxCalculator {
public:
    MinMaxCalculator();
    ~MinMaxCalculator();

public:
    void Add(double value);
    void Reset();
    std::string ToString() const;

    double Min() const { return min_; }

    double Max() const { return max_; }

    double Sum() const { return sum_; }

    int Count() const { return count_; }

private:
    MinMaxCalculator(const MinMaxCalculator &);
    MinMaxCalculator &operator=(const MinMaxCalculator &);

private:
    int count_;
    double min_;
    double max_;
    double sum_;
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_COMMON_MINMAXCALCULATOR_H_
