/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 10:47
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#ifndef KMONITOR_CLIENT_METRIC_DDSKETCH_H_
#define KMONITOR_CLIENT_METRIC_DDSKETCH_H_

#include <unordered_map>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/metric/DenseStore.h"
#include "kmonitor/client/metric/LogarithmicMapping.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class DDSketch : public autil::legacy::Jsonizable {
public:
    DDSketch(double relativeAccuracy, double minIndexedValue = 0);
    ~DDSketch() {}
    void accept(double value);
    void Flush();
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;

private:
    LogarithmicMapping indexMapping_;
    DenseStore store_;
    std::unordered_map<int32_t, int32_t> indexCountMap_;
    double minIndexedValue_;
    double maxIndexedValue_;
    int32_t zeroCount_;
    static const size_t DDSKETCH_BATCH_SIZE;
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_METRIC_DDSKETCH_H_
