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
#ifndef FSLIB_SPEEDCONTROLLER_H
#define FSLIB_SPEEDCONTROLLER_H

#include <autil/Log.h>
#include <autil/Lock.h>
#include "fslib/fslib.h"

FSLIB_BEGIN_NAMESPACE(fs);

class SpeedController
{
public:
    SpeedController();
    ~SpeedController();

public:
    // format : quota_size=10485760,quota_interval=1000
    void initFromString(const std::string& paramStr);
    bool matchPathPattern(const std::string& path) const;
    int64_t reserveQuota(int64_t quota);
    const std::string& getPathPattern() const { return _pathPattern; }
    int64_t getQuotaPerSecond() const;
    void resetQuotaPerSecond(int64_t quota);

    double getQuotaUsageRatio() const { return _quotaUsageRatio; }

// for test
public:
    int64_t TEST_getInterval() const;
    std::string TEST_getPathPattern() const;
    bool TEST_isInit() const;
    uint32_t TEST_getQuotaSize() const;
    size_t TEST_getTotalWaitCount() const;

private:
    bool _inited;
    volatile uint32_t _quotaSizeInByte;
    volatile double _quotaUsageRatio;
    uint32_t _interval;
    int64_t _baseLineTs;
    int64_t _remainQuota;
    size_t _totalWaitCount;
    std::string _pathPattern;
    autil::ThreadMutex _lock;
};

FSLIB_TYPEDEF_AUTO_PTR(SpeedController);

FSLIB_END_NAMESPACE(fs);

#endif //FSLIB_SPEEDCONTROLLER_H
