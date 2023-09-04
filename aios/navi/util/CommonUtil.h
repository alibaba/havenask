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
#ifndef NAVI_COMMONUTIL_H
#define NAVI_COMMONUTIL_H

#include "navi/common.h"
#include "navi/log/NaviLogger.h"

namespace navi {

class CommonUtil
{
public:
    CommonUtil();
    ~CommonUtil();
private:
    CommonUtil(const CommonUtil &);
    CommonUtil &operator=(const CommonUtil &);
public:
    static std::string getPortKey(const std::string &node, const std::string &port);
    static bool parseGroupPort(const std::string &port, std::string &name,
                               IndexType &index);
    static std::string getGroupPortName(const std::string &port, IndexType index);
    static std::string getGraphPortName(const std::string &node,
                                        const std::string &port);
    static const char *getErrorString(ErrorCode ec);
    static const char *getPortQueueType(PortQueueType type);
    static const char *getStoreType(PortStoreType type);
    static const char *getIoType(IoType type);
    static IoType getReverseIoType(IoType type);
    static const char *getActivateStrategy(ActivateStrategy as);
    static uint64_t random64();
    static std::string formatInstanceId(InstanceId instance);
    static std::string formatSessionId(SessionId id, InstanceId thisInstance);
    static std::string getConfigAbsPath(const std::string &path,
            const std::string &file);
    static std::string getMirrorBizName(GraphId graphId);
    static int64_t getTimelineTimeNs() {
        return getMonotonicTimeNs();
    }
    static int64_t getMonotonicTimeNs() {
        struct timespec spec;
        clock_gettime(CLOCK_MONOTONIC, &spec);
        return spec.tv_sec * 1000000000L + spec.tv_nsec;
    }
    template <typename T>
    static void waitUseCount(std::shared_ptr<T> &ptr,
                             size_t expectUseCount = 1,
                             int64_t timeout = std::numeric_limits<int64_t>::max());
};

template <typename T>
inline void CommonUtil::waitUseCount(std::shared_ptr<T> &ptr,
                                     size_t expectUseCount,
                                     int64_t timeout)
{
    constexpr int64_t interval = 50 * 1000; // 50ms
    assert(ptr && "wait ptr should not be null");
    size_t sleepCount = 0;
    auto &type = typeid(*ptr);
    NAVI_KERNEL_LOG(INFO, "wait use count to [%lu] start, ptr [%p] type [%s], quota [%ld]",
                    expectUseCount, ptr.get(), type.name(), timeout);
    while (true) {
        auto count = ptr.use_count();
        if (count <= expectUseCount) {
            NAVI_KERNEL_LOG(INFO, "wait use count to [%lu] succeed, current [%lu], "
                        "ptr [%p] type [%s], quota [%ld]",
                        expectUseCount, count, ptr.get(), type.name(), timeout);
            break;
        }
        if (timeout <= 0) {
            NAVI_KERNEL_LOG(INFO, "wait use count to [%lu] timeout, current [%lu], "
                            "ptr [%p] type [%s], quota [%ld]",
                            expectUseCount, count, ptr.get(), type.name(), timeout);
            break;
        }
        if (sleepCount++ % 20 == 0) {
            NAVI_KERNEL_LOG(INFO, "wait use count to [%lu], current [%lu], "
                            "ptr [%p] type [%s], quota [%ld]",
                            expectUseCount, count, ptr.get(), type.name(), timeout);
        }
        usleep(interval);
        timeout -= interval;
    }
    NAVI_KERNEL_LOG(INFO, "wait use count to [%lu] finished, sleepCount [%lu], "
                    "ptr [%p] type [%s], quota [%ld]",
                    expectUseCount, sleepCount, ptr.get(), type.name(), timeout);
}

}

#endif //NAVI_COMMONUTIL_H
