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

#include <cstddef>
#include <string>

#include "autil/Lock.h"
#include "autil/Thread.h"

namespace autil {

class ThreadAutoScaler {
public:
    ThreadAutoScaler();
    ~ThreadAutoScaler();

public:
    bool init(const std::string &threadName);
    void stop();
    void wait(size_t index);
    size_t getMaxThreadNum() const;
    size_t getMinThreadNum() const;
    size_t getActiveThreadNum() const;

private:
    bool initByEnv();
    void autoScaleThread();
    bool updateActiveThreadCount();
    void signal();

private:
    static bool calcActiveThreadCount(size_t &activeCount);

private:
    size_t mMinActiveThreadNum;
    size_t mMaxActiveThreadNum;
    volatile size_t mActiveThreadCount;
    autil::ThreadCond *mCpuScaleNotifier;
    bool mStopped;
    autil::ThreadPtr mAutoScaleThread;

private:
    static constexpr size_t DEFAULT_THREAD_NUM_MIN = 20;
    static constexpr size_t DEFAULT_THREAD_NUM_MAX = 96;
    static const std::string THREAD_NUM_RANGE_SEPERATOR;

public:
    static const std::string THREAD_NUM_RANGE_ENV;
};

} // namespace autil
