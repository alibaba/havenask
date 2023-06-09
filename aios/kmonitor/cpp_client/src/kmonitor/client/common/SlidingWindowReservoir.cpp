/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-09-06 10:00
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#include <algorithm>
#include "kmonitor/client/common/UniformSnapshot.h"
#include "kmonitor/client/common/SlidingWindowReservoir.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);


SlidingWindowReservoir::SlidingWindowReservoir(int64_t size) {
    size_ = size;
    count_ = 0;
    measurements_ = new double[size_];
}

SlidingWindowReservoir::~SlidingWindowReservoir() {
    if (measurements_ != NULL) {
        delete[] measurements_;
        measurements_ = NULL;
    }
}

int32_t SlidingWindowReservoir::Size() const {
    return static_cast<int32_t>(std::min(count_, size_));
}

void SlidingWindowReservoir::Add(double value) {
    int32_t pos = static_cast<int32_t>(count_++ % size_);
    measurements_[pos] = value;
}

double SlidingWindowReservoir::GetValue(int32_t pos) {
    if (pos + 1 > size_) {
        return 0.0;
    }
    return measurements_[pos];
}

UniformSnapshot* SlidingWindowReservoir::GetSnapShot() {
    return new UniformSnapshot(measurements_, Size());
}

void SlidingWindowReservoir::Reset() {
    count_ = 0;
}



END_KMONITOR_NAMESPACE(kmonitor);

