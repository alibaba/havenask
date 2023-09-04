/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 10:47
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#include "kmonitor/client/metric/DDSketch.h"

#include <map>
#include <set>
#include <string>
#include <unordered_map>

#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/metric/DenseStore.h"
#include "kmonitor/client/metric/LogarithmicMapping.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);
using namespace std;

const size_t DDSketch::DDSKETCH_BATCH_SIZE = 100;

DDSketch::DDSketch(double relativeAccuracy, double minIndexedValue) : indexMapping_(relativeAccuracy) {
    minIndexedValue_ =
        (minIndexedValue > indexMapping_.minIndexableValue()) ? minIndexedValue : indexMapping_.minIndexableValue();
    maxIndexedValue_ = indexMapping_.maxIndexableValue();
    zeroCount_ = 0;
    indexCountMap_.reserve(DDSKETCH_BATCH_SIZE);
}

void DDSketch::accept(double value) {
    if (value < 0 || value > maxIndexedValue_) {
        return;
    }
    if (value < minIndexedValue_) {
        zeroCount_++;
    } else {
        indexCountMap_[indexMapping_.index(value)]++;
        if (indexCountMap_.size() >= DDSKETCH_BATCH_SIZE) {
            Flush();
        }
    }
}

void DDSketch::Flush() {
    for (auto indexCount : indexCountMap_) {
        store_.add(indexCount.first, indexCount.second);
    }
    indexCountMap_.clear();
}

// use autil::legacy::ToJsonString(DDSketch, true) to get
void DDSketch::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("minIndexedValue", minIndexedValue_, minIndexedValue_);
    json.Jsonize("maxIndexedValue", maxIndexedValue_, maxIndexedValue_);
    json.Jsonize("zeroCount", zeroCount_, zeroCount_);
    int32_t arrayLengthGrowthIncrement = store_.getArrayLengthGrowthIncrement();
    json.Jsonize("arrayLengthGrowthIncrement", arrayLengthGrowthIncrement, arrayLengthGrowthIncrement);
    int32_t arrayLengthOverhead = store_.getArrayLengthOverhead();
    json.Jsonize("arrayLengthOverhead", arrayLengthOverhead, arrayLengthOverhead);
    int32_t offset = store_.getOffset();
    json.Jsonize("offset", offset, offset);
    int32_t minIndex = store_.getMinIndex();
    json.Jsonize("minIndex", minIndex, minIndex);
    int32_t maxIndex = store_.getMaxIndex();
    json.Jsonize("maxIndex", maxIndex, maxIndex);
    std::vector<int64_t> counts;
    store_.getCounts(counts);
    json.Jsonize("counts", counts, counts);
    double relativeAccuracy = indexMapping_.relativeAccuracy();
    json.Jsonize("relativeAccuracy", relativeAccuracy, relativeAccuracy);
}

END_KMONITOR_NAMESPACE(kmonitor);
