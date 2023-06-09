/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-09-06 10:35
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#ifndef KMONITOR_CLIENT_COMMON_UNIFORMSNAPSHOT_H_
#define KMONITOR_CLIENT_COMMON_UNIFORMSNAPSHOT_H_

#include <vector>
#include "kmonitor/client/common/Common.h"
BEGIN_KMONITOR_NAMESPACE(kmonitor);

class UniformSnapshot {
 public:
    UniformSnapshot(const double *measurements, int32_t size);
    ~UniformSnapshot();

    double GetValue(double quantile);

 private:
    UniformSnapshot(const UniformSnapshot &);
    UniformSnapshot& operator=(const UniformSnapshot &);

 private:
    std::vector<double> *values_;
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif  // KMONITOR_CLIENT_COMMON_UNIFORMSNAPSHOT_H_
