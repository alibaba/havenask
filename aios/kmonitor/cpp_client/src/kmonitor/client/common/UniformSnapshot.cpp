/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-09-06 10:35
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#include <math.h>
#include <vector>
#include <algorithm>
#include "kmonitor/client/common/UniformSnapshot.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);


UniformSnapshot::UniformSnapshot(const double *measurements, int32_t size) {
    values_ = new std::vector<double>(measurements, measurements+size);
    std::sort(values_->begin(), values_->end());
}

UniformSnapshot::~UniformSnapshot() {
    if (values_ != NULL) {
        delete values_;
        values_ = NULL;
    }
}

double UniformSnapshot::GetValue(double quantile) {
    assert(quantile > 0.0);
    assert(quantile < 1.0);

    if (values_->size() == 0) {
        return 0.0;
    }

    double pos = quantile * (values_->size() + 1);
    size_t index = static_cast<size_t>(pos);

    if (index < 1) {
        return values_->at(0);
    }

    if (index >= values_->size()) {
        return values_->at(values_->size() - 1);
    }

    double lower = values_->at(index-1);
    double upper = values_->at(index);
    return lower + (pos - floor(pos)) * (upper - lower);
}

END_KMONITOR_NAMESPACE(kmonitor);

