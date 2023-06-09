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
#include "autil/ThreadAutoScaler.h"

#include <cassert>
#include <cstdlib>
#include <algorithm>
#include <functional>
#include <memory>
#include <vector>

#include <unistd.h>
#include <errno.h>

#include "autil/StringUtil.h"
#include "autil/Lock.h"
#include "autil/Log.h"

namespace autil {

AUTIL_DECLARE_AND_SETUP_LOGGER(autil, ThreadAutoScaler);

const std::string ThreadAutoScaler::THREAD_NUM_RANGE_SEPERATOR = ":";
const std::string ThreadAutoScaler::THREAD_NUM_RANGE_ENV = "arpc_auto_scale_thread_num_range";

ThreadAutoScaler::ThreadAutoScaler()
    : mMinActiveThreadNum(0)
    , mMaxActiveThreadNum(0)
    , mActiveThreadCount(0)
    , mCpuScaleNotifier(nullptr)
    , mStopped(false)
{
}

ThreadAutoScaler::~ThreadAutoScaler() {
    stop();
    delete[] mCpuScaleNotifier;
}

bool ThreadAutoScaler::init(const std::string &threadName) {
    if (!initByEnv()) {
        return false;
    }
    if (!updateActiveThreadCount()) {
        return false;
    }
    mCpuScaleNotifier = new autil::ThreadCond[mMaxActiveThreadNum];
    mAutoScaleThread = autil::Thread::createThread(
        std::bind(&ThreadAutoScaler::autoScaleThread, this),
        threadName + "AutoScale");
    if (!mAutoScaleThread) {
        AUTIL_LOG(ERROR, "create auto scale thread failed");
        return false;
    }
    AUTIL_LOG(INFO, "auto scale init success, active [%lu], thread num range min[%lu], max[%lu]",
             mActiveThreadCount, mMinActiveThreadNum, mMaxActiveThreadNum);
    return true;
}

bool ThreadAutoScaler::initByEnv() {
    const char *rangeStr = getenv(THREAD_NUM_RANGE_ENV.c_str());
    if (!rangeStr) {
        mMinActiveThreadNum = DEFAULT_THREAD_NUM_MIN;
        mMaxActiveThreadNum = DEFAULT_THREAD_NUM_MAX;
        return true;
    }
    auto splited = StringUtil::split(std::string(rangeStr), THREAD_NUM_RANGE_SEPERATOR);
    if (2u != splited.size()) {
        AUTIL_LOG(ERROR, "invalid thread num range[%s], field count is not 2", rangeStr);
        return false;
    }
    const auto &minStr = splited[0];
    const auto &maxStr = splited[1];
    size_t minFromEnv = 0;
    size_t maxFromEnv = 0;
    if (!StringUtil::strToUInt64(minStr.c_str(), minFromEnv) ||
        !StringUtil::strToUInt64(maxStr.c_str(), maxFromEnv))
    {
        AUTIL_LOG(ERROR, "invalid range[%s], parse number failed", rangeStr);
        return false;
    }
    if (maxFromEnv < minFromEnv) {
        AUTIL_LOG(ERROR, "invalid range[%s], max[%lu] is smaller than min[%lu]",
                 rangeStr, maxFromEnv, minFromEnv);
        return false;
    }
    mMinActiveThreadNum = minFromEnv;
    mMaxActiveThreadNum = maxFromEnv;
    AUTIL_LOG(INFO, "get thread num range from env [%s] success", rangeStr);
    return true;
}

void ThreadAutoScaler::stop() {
    mActiveThreadCount = mMaxActiveThreadNum;
    signal();
    mStopped = true;
    if (mAutoScaleThread) {
        mAutoScaleThread->join();
        mAutoScaleThread.reset();
    }
}

void ThreadAutoScaler::wait(size_t index) {
    if (index < mActiveThreadCount) {
        // active
        return;
    }
    if (index < mMaxActiveThreadNum && mCpuScaleNotifier) {
        // inactive
        AUTIL_LOG(INFO, "thread [%lu] suspended", index);
        auto &cond = mCpuScaleNotifier[index];
        cond.lock();
        cond.wait();
        cond.unlock();
        AUTIL_LOG(INFO, "thread [%lu] resumed", index);
    } else {
        // never reach
        assert(false);
    }
}

void ThreadAutoScaler::autoScaleThread() {
    while (true) {
        if (mStopped) {
            break;
        }
        updateActiveThreadCount();
        signal();
        usleep(1000 * 1000);
    }
}

void ThreadAutoScaler::signal() {
    if (!mCpuScaleNotifier) {
        return;
    }
    size_t activeCount = mActiveThreadCount;
    activeCount = std::min(activeCount, mMaxActiveThreadNum);
    for (size_t i = 0; i < activeCount; i++) {
        auto &cond = mCpuScaleNotifier[i];
        cond.lock();
        cond.signal();
        cond.unlock();
    }
}

bool ThreadAutoScaler::updateActiveThreadCount() {
    size_t newThreadCount = mMaxActiveThreadNum;
    if (!calcActiveThreadCount(newThreadCount)) {
        return false;
    }
    newThreadCount = std::min(newThreadCount, mMaxActiveThreadNum);
    newThreadCount = std::max(newThreadCount, mMinActiveThreadNum);
    if (newThreadCount != mActiveThreadCount) {
        AUTIL_LOG(INFO, "active thread count changed from[%lu] to [%lu]",
                 mActiveThreadCount, newThreadCount);
    }
    mActiveThreadCount = newThreadCount;
    return true;
}

bool ThreadAutoScaler::calcActiveThreadCount(size_t &activeCount) {
    errno = 0;
    auto coreNum = sysconf(_SC_NPROCESSORS_ONLN);
    if (errno == 0 && coreNum > 0) {
        activeCount = coreNum;
        return true;
    } else {
        AUTIL_LOG(ERROR, "auto scale get core num failed");
        return false;
    }
}

size_t ThreadAutoScaler::getMaxThreadNum() const {
    return mMaxActiveThreadNum;
}

size_t ThreadAutoScaler::getMinThreadNum() const {
    return mMinActiveThreadNum;
}

size_t ThreadAutoScaler::getActiveThreadNum() const {
    return mActiveThreadCount;
}

} // namespace autil
