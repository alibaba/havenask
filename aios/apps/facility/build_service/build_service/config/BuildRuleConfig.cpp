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
#include "build_service/config/BuildRuleConfig.h"

#include <cstdint>
#include <iosfwd>

#include "alog/Logger.h"

using namespace std;
namespace build_service { namespace config {
BS_LOG_SETUP(config, BuildRuleConfig);

BuildRuleConfig::BuildRuleConfig()
    : partitionCount(1)
    , buildParallelNum(1)
    , incBuildParallelNum(1)
    , mergeParallelNum(1)
    , mapReduceRatio(1)
    , partitionSplitNum(1)
    , needPartition(false)
    , alignVersion(true)
    , disableIncParallelInstanceDir(false)
{
}

BuildRuleConfig::~BuildRuleConfig() {}

void BuildRuleConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("partition_count", partitionCount, partitionCount);
    json.Jsonize("build_parallel_num", buildParallelNum, buildParallelNum);
    json.Jsonize("inc_build_parallel_num", incBuildParallelNum, incBuildParallelNum);
    json.Jsonize("merge_parallel_num", mergeParallelNum, mergeParallelNum);
    json.Jsonize("map_reduce_ratio", mapReduceRatio, mapReduceRatio);
    json.Jsonize("partition_split_num", partitionSplitNum, partitionSplitNum);
    json.Jsonize("need_partition", needPartition, needPartition);
    json.Jsonize("align_version", alignVersion, alignVersion);
    json.Jsonize("disable_inc_parallel_instance_dir", disableIncParallelInstanceDir, disableIncParallelInstanceDir);
}

#define CHECK_CONFIG(condition, errorMsg)                                                                              \
    if (!(condition)) {                                                                                                \
        BS_LOG(ERROR, "%s", errorMsg);                                                                                 \
        return false;                                                                                                  \
    }

bool BuildRuleConfig::validate() const
{
    CHECK_CONFIG(partitionCount > 0, "partition count must greater than 0!");
    CHECK_CONFIG(buildParallelNum > 0, "build parallel num must greater than 0!");
    CHECK_CONFIG(incBuildParallelNum > 0, "inc build parallel num must greater than 0!");
    CHECK_CONFIG(mergeParallelNum > 0, "merge parallel num must greater than 0!");
    CHECK_CONFIG(mapReduceRatio > 0, "map reduce ratio must greater than 0!");
    CHECK_CONFIG(partitionSplitNum > 0, "partition split num must greater than 0!");
    return true;
}

}} // namespace build_service::config
