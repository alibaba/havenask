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

#include <stddef.h>
#include <stdint.h>

#include "autil/legacy/jsonizable.h"
#include "build_service/util/Log.h"
#include "indexlib/config/SortDescription.h"

namespace build_service { namespace config {

class BuilderConfig : public autil::legacy::Jsonizable
{
public:
    BuilderConfig();
    ~BuilderConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool validate() const;
    int64_t calculateSortQueueMemory(size_t buildTotalMemMB) const
    {
        if (sortQueueMem < 0) {
            return (int64_t)(buildTotalMemMB * SORT_QUEUE_MEM_FACTOR);
        }
        return sortQueueMem / 2;
    }

public:
    bool operator==(const BuilderConfig& other) const;

public:
    int64_t sortQueueMem;  // in MB
    int64_t asyncQueueMem; // in MB
    bool sortBuild;
    bool asyncBuild;
    bool documentFilter;
    uint32_t asyncQueueSize;
    uint32_t sortQueueSize; // for test
    int sleepPerdoc;        // for test
    int buildQpsLimit;
    size_t batchBuildSize;
    // TODO: Remove thread count config here and let suez initialize global thread pool after suez supports this design.
    int32_t consistentModeBuildThreadCount;
    int32_t inconsistentModeBuildThreadCount;
    indexlibv2::config::SortDescriptions sortDescriptions;

public:
    static const uint32_t DEFAULT_SORT_QUEUE_SIZE;
    static const uint32_t DEFAULT_ASYNC_QUEUE_SIZE;
    static const int64_t INVALID_SORT_QUEUE_MEM;
    static const int64_t INVALID_ASYNC_QUEUE_MEM;
    static constexpr double SORT_QUEUE_MEM_FACTOR = 0.3;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::config
