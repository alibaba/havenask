/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2019-05-28 16:53
 * Author Name: 夕奇
 * Author Email: xiqi.lq@alibaba-inc.com
 */

#include "aios/network/gig/multi_call/service/LatencyTimeWindow.h"

namespace multi_call {

LatencyTimeWindow::LatencyTimeWindow(int64_t windowSize)
    : _windowSize(windowSize) {}

LatencyTimeWindow::~LatencyTimeWindow() {}

/* 指数移动平均,变种的加权移动平均
 * EMA = (current - EMA) / window + EMA
 * window越大对于趋势体现越不明显,
 * 在sap场景下,主要功能是平滑突刺,可以不敏感,
 * 在初始化过程中会由敏感->不敏感的变化,目前直接略去
 */
int64_t LatencyTimeWindow::push(int64_t latency) {
    int64_t avg = _currentAvg.getValue();
    int64_t value = (latency << 10) + (avg * (_windowSize - 1));
    value /= _windowSize;
    _currentAvg.setValue(value);
    return value >> 10;
}

} // namespace multi_call
