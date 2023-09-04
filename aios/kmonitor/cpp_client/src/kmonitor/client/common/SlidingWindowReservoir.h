/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-09-06 10:00
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#ifndef KMONITOR_CLIENT_COMMON_SLIDINGWINDOWRESERVOIR_H_
#define KMONITOR_CLIENT_COMMON_SLIDINGWINDOWRESERVOIR_H_

#include "kmonitor/client/common/Common.h"
BEGIN_KMONITOR_NAMESPACE(kmonitor);

class UniformSnapshot;

class SlidingWindowReservoir {
public:
    explicit SlidingWindowReservoir(int64_t size = 1000);
    ~SlidingWindowReservoir();

    int32_t Size() const;
    void Add(double value);
    UniformSnapshot *GetSnapShot();
    // for test
    double GetValue(int32_t pos);
    void Reset();

private:
    SlidingWindowReservoir(const SlidingWindowReservoir &);
    SlidingWindowReservoir &operator=(const SlidingWindowReservoir &);

private:
    double *measurements_;
    int64_t count_;
    int64_t size_;
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_COMMON_SLIDINGWINDOWRESERVOIR_H_
