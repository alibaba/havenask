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
#ifndef __INDEXLIB_MEM_UTIL_H
#define __INDEXLIB_MEM_UTIL_H

#include <fstream>
#include <memory>

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"

namespace indexlib { namespace util {

class MemUtil
{
public:
    MemUtil() {}
    ~MemUtil() {}

    MemUtil(const MemUtil&) = delete;
    MemUtil& operator=(const MemUtil&) = delete;
    MemUtil(MemUtil&&) = delete;
    MemUtil& operator=(MemUtil&&) = delete;

public:
    static int64_t GetContainerMemoryLimitsInBytes()
    {
        int64_t memTotal = -1;
        std::string memoryInfoPath =
            autil::EnvUtil::getEnv("INDEXLIB_CONTAINER_MEMORY_INFO_FILE_PATH", std::string("/proc/meminfo"));
        std::ifstream fin(memoryInfoPath);
        if (!fin) {
            return -1;
        }
        std::string line;
        std::vector<std::string> items;
        items.reserve(3);
        while (getline(fin, line)) {
            items.clear();
            autil::StringUtil::split(items, line, " ");
            int64_t memTotalInKb = -1;
            if (items.size() != 3) {
                continue;
            }
            if (items[0] == "MemTotal:") {
                if (autil::StringUtil::strToInt64(items[1].c_str(), memTotalInKb)) {
                    if (memTotalInKb <= 0) {
                        return -1;
                    }
                    memTotal = memTotalInKb * 1024;
                    return memTotal;
                } else {
                    return -1;
                }
            }
        }
        return -1;
    }
};

}} // namespace indexlib::util

#endif //__INDEXLIB_MEM_UTIL_H
