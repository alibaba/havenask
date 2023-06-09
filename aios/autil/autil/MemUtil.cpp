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
#include "autil/MemUtil.h"

#include <fstream>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/StringUtil.h"

using namespace std;

namespace autil {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, MemUtil);

uint64_t MemUtil::getMachineTotalMem() {
    ifstream fin("/proc/meminfo");
    if (!fin) {
        AUTIL_LOG(WARN, "Failed to open file /proc/meminfo");
        return 0;
    }
    map<string, string> memInfos;
    string line;
    while (getline(fin, line)) {
        vector<string> items = StringUtil::split(line, " ");
        if (items.size() != 3) {
            continue;
        }
        memInfos[items[0]] = items[1];
    }
    auto it = memInfos.find("MemTotal:");
    if (it != memInfos.end()) {
        uint64_t memTotal = 0;
        StringUtil::strToUInt64(it->second.c_str(), memTotal);
        return memTotal * 1024;
    } else {
        AUTIL_LOG(WARN, "Meminfo dont have MemTotal");
        return 0;
    }
}

} // namespace autil
