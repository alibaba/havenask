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

#include <cstdint>
#include <stddef.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "kmonitor/client/core/MetricsTags.h"

namespace build_service { namespace admin {

class MetricEventLogger
{
public:
    MetricEventLogger(int64_t obsoletTime = 10 /* 10s */);

public:
    void update(int64_t value, const kmonitor::MetricsTags& tags);
    void fillLogInfo(std::vector<std::pair<std::string, int64_t>>& values) const;
    void clear();

private:
    void cleanObsoleteLog(int64_t currentTs);

private:
    std::unordered_map<std::string, size_t> _idxMap;
    std::vector<std::pair<int64_t, int64_t>> _dataVec;
    int64_t _obsoleteTs;
    mutable int64_t _lastCleanTs;
    mutable autil::RecursiveThreadMutex _lock;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MetricEventLogger);

}} // namespace build_service::admin
