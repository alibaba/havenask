/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2020-01-03 10:47
 * Author Name: yixuan.ly
 * Author Email: yixuan.ly@alibaba-inc.com
 */

#include "kmonitor/client/metric/DenseStore.h"

#include <limits.h>
#include <string.h>
#include <string>
#include <vector>

#include "kmonitor/client/common/Common.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

DenseStore::DenseStore(int32_t arrayLengthGrowthIncrement, int32_t arrayLengthOverhead)
    : arrayLengthGrowthIncrement_(arrayLengthGrowthIncrement), arrayLengthOverhead_(arrayLengthOverhead) {
    counts_ = NULL;
    countsLength_ = 0;
    offset_ = 0;
    minIndex_ = INT_MAX; // for empty
    maxIndex_ = INT_MIN;
}

DenseStore::~DenseStore() { delete[] counts_; }

void DenseStore::add(int32_t index, int64_t count) {
    if (count <= 0) {
        return;
    }

    int32_t arrayIndex = normalize(index);
    counts_[arrayIndex] += count;
}

int64_t DenseStore::getTotalCount(int32_t fromIndex, int32_t toIndex) {
    if (isEmpty()) {
        return 0;
    }

    int32_t fromArrayIndex = (fromIndex > offset_) ? (fromIndex - offset_) : 0;
    int32_t toArrayIndex = ((toIndex - offset_) < (countsLength_ - 1)) ? (toIndex - offset_) : (countsLength_ - 1);

    int64_t totalCount = 0;
    for (int32_t arrayIndex = fromArrayIndex; arrayIndex <= toArrayIndex; arrayIndex++) {
        totalCount += (int64_t)counts_[arrayIndex];
    }

    return totalCount;
}

void DenseStore::extendRange(int32_t newMinIndex, int32_t newMaxIndex) {
    newMinIndex = (newMinIndex < minIndex_) ? newMinIndex : minIndex_;
    newMaxIndex = (newMaxIndex > maxIndex_) ? newMaxIndex : maxIndex_;
    if (isEmpty()) {
        countsLength_ = getNewLength(newMaxIndex - newMinIndex + 1);
        counts_ = new int64_t[countsLength_](); // add () to init 0 in counts_
        offset_ = newMinIndex;
        minIndex_ = newMinIndex;

        // java就是取的newMinIndex，而且因为newMinIndex==newMaxIndex，所以暂保持一致
        maxIndex_ = newMinIndex;
        adjust(newMinIndex, newMaxIndex);
    } else if ((newMinIndex >= offset_ && newMaxIndex < offset_ + countsLength_)) {
        minIndex_ = newMinIndex;
        maxIndex_ = newMaxIndex;
    } else {
        int32_t desiredLength = newMaxIndex - newMinIndex + 1;
        int32_t newLength = getNewLength(desiredLength);
        if (newLength > countsLength_) {
            int64_t *newCounts = new int64_t[newLength]();
            memcpy((void *)newCounts, (void *)counts_, countsLength_ * sizeof(int64_t));
            countsLength_ = newLength;
            delete[] counts_;
            counts_ = newCounts;
        }

        adjust(newMinIndex, newMaxIndex);
    }
}

void DenseStore::centerCounts(int32_t newMinIndex, int32_t newMaxIndex) {
    int32_t middleIndex = newMinIndex + (newMaxIndex - newMinIndex + 1) / 2;
    shiftCounts(offset_ + countsLength_ / 2 - middleIndex);

    minIndex_ = newMinIndex;
    maxIndex_ = newMaxIndex;
}

void DenseStore::getCounts(std::vector<int64_t> &vec) { vec.assign(counts_, counts_ + countsLength_); }

void DenseStore::shiftCounts(int32_t shift) {
    int32_t minArrayIndex = minIndex_ - offset_;
    int32_t maxArrayIndex = maxIndex_ - offset_;

    memmove((void *)(counts_ + (minArrayIndex + shift)),
            (void *)(counts_ + minArrayIndex),
            (maxArrayIndex - minArrayIndex + 1) * sizeof(int64_t));

    if (shift > 0) {
        memset((void *)(counts_ + minArrayIndex), 0, shift * sizeof(int64_t));
    } else {
        memset((void *)(counts_ + (maxArrayIndex + shift + 1)), 0, (-shift) * sizeof(int64_t));
    }

    //初始及每次新申请空间时offset_ = 0 放的index即为midindex_value，这样就尽可能多存数据
    // offset_ - (offset_ + countsLength_ / 2 - middleIndex) = middleIndex - countsLength_ / 2
    offset_ -= shift;
}

int32_t DenseStore::normalize(int32_t index) {
    if (index < minIndex_ || index > maxIndex_) {
        extendRange(index);
    }

    return index - offset_;
}

void DenseStore::adjust(int32_t newMinIndex, int32_t newMaxIndex) { centerCounts(newMinIndex, newMaxIndex); }

END_KMONITOR_NAMESPACE(kmonitor);
