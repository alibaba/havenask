/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <memory>

namespace indexlib { namespace util {

class ProgressMetrics
{
public:
    ProgressMetrics(double total) : _total(total), _currentRatio(0.0f), _startTimestamp(0) {}

    ~ProgressMetrics() {}

public:
    void SetStartTimestamp(int64_t ts) { _startTimestamp = ts; }
    int64_t GetStartTimestamp() const { return _startTimestamp; }

    void SetTotal(double total) { _total = total; }

    void UpdateCurrentRatio(double ratio)
    {
        assert(ratio <= 1.0f && ratio >= 0.0f);
        assert(ratio >= _currentRatio);
        _currentRatio = ratio;
    }

    void SetFinish() { UpdateCurrentRatio(1.0f); }

    double GetTotal() const { return _total; }
    double GetCurrentRatio() const { return _currentRatio; }
    double GetCurrent() const { return _total * _currentRatio; }

    bool IsFinished() const { return _currentRatio >= 1.0f; }

private:
    double _total;
    volatile double _currentRatio;
    volatile int64_t _startTimestamp;
};

typedef std::shared_ptr<ProgressMetrics> ProgressMetricsPtr;
}} // namespace indexlib::util
