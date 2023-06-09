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
#include "build_service/config/BuilderConfig.h"

using namespace std;
namespace build_service { namespace config {

BS_LOG_SETUP(config, BuilderConfig);

const uint32_t BuilderConfig::DEFAULT_SORT_QUEUE_SIZE = numeric_limits<uint32_t>::max();
const uint32_t BuilderConfig::DEFAULT_ASYNC_QUEUE_SIZE = 10000;
const int64_t BuilderConfig::INVALID_SORT_QUEUE_MEM = -1;
const int64_t BuilderConfig::INVALID_ASYNC_QUEUE_MEM = -1;

BuilderConfig::BuilderConfig()
    : sortQueueMem(INVALID_SORT_QUEUE_MEM)
    , asyncQueueMem(INVALID_ASYNC_QUEUE_MEM)
    , sortBuild(false)
    , asyncBuild(false)
    , documentFilter(true)
    , asyncQueueSize(DEFAULT_ASYNC_QUEUE_SIZE)
    , sortQueueSize(DEFAULT_SORT_QUEUE_SIZE)
    , sleepPerdoc(0)
    , buildQpsLimit(0)
    , batchBuildSize(1)
    , consistentModeBuildThreadCount(-1)
    , inconsistentModeBuildThreadCount(-1)
{
}

BuilderConfig::~BuilderConfig() {}

void BuilderConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("sort_build", sortBuild, sortBuild);
    json.Jsonize("document_filter", documentFilter, documentFilter);
    json.Jsonize("sort_queue_size", sortQueueSize, sortQueueSize);
    json.Jsonize("sort_queue_mem", sortQueueMem, sortQueueMem);
    json.Jsonize("async_queue_mem", asyncQueueMem, asyncQueueMem);
    json.Jsonize("sort_descriptions", sortDescriptions, sortDescriptions);
    json.Jsonize("async_queue_size", asyncQueueSize, asyncQueueSize);
    json.Jsonize("async_build", asyncBuild, asyncBuild);
    json.Jsonize("sleep_per_doc", sleepPerdoc, sleepPerdoc);
    json.Jsonize("build_qps_limit", buildQpsLimit, buildQpsLimit);
    json.Jsonize("batch_build_size", batchBuildSize, batchBuildSize);
    json.Jsonize("consistent_mode_build_thread_count", consistentModeBuildThreadCount, consistentModeBuildThreadCount);
    json.Jsonize("inconsistent_mode_build_thread_count", inconsistentModeBuildThreadCount,
                 inconsistentModeBuildThreadCount);
}

bool BuilderConfig::operator==(const BuilderConfig& other) const
{
    return sortBuild == other.sortBuild && documentFilter == other.documentFilter &&
           sortQueueSize == other.sortQueueSize && sortQueueMem == other.sortQueueMem &&
           asyncQueueMem == other.asyncQueueMem && sortDescriptions == other.sortDescriptions &&
           asyncBuild == other.asyncBuild && asyncQueueSize == other.asyncQueueSize &&
           batchBuildSize == other.batchBuildSize &&
           consistentModeBuildThreadCount == other.consistentModeBuildThreadCount &&
           inconsistentModeBuildThreadCount == other.inconsistentModeBuildThreadCount;
}

bool BuilderConfig::validate() const
{
    if (sortBuild && sortQueueSize == 0) {
        string errorMsg = "sort build with sort queue size zero";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (asyncBuild && asyncQueueSize == 0) {
        string errorMsg = "async build with async queue size zero";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (batchBuildSize == 0) {
        string errorMsg = "batch build size zero";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    // delay validate sort_descriptions
    return true;
}

}} // namespace build_service::config
