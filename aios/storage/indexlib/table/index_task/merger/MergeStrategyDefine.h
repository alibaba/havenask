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

#include <memory>
#include <string>

#include "autil/Log.h"

namespace indexlibv2 { namespace table {

class MergeStrategyDefine
{
public:
    static constexpr char COMBINED_MERGE_STRATEGY_NAME[] = "combined";
    static constexpr char PRIORITY_QUEUE_MERGE_STRATEGY_NAME[] = "priority_queue";
    static constexpr char SPLIT_STRATEGY_NAME[] = "split";
    static constexpr char REALTIME_MERGE_STRATEGY_NAME[] = "realtime";
    static constexpr char OPTIMIZE_MERGE_STRATEGY_NAME[] = "optimize";
    static constexpr char SHARD_BASED_MERGE_STRATEGY_NAME[] = "shard_based";
    static constexpr char BALANCE_TREE_MERGE_STRATEGY_NAME[] = "balance_tree";
    static constexpr char SPECIFIC_SEGMENTS_MERGE_STRATEGY_NAME[] = "specific_segments";
    static constexpr char LEVELED_COMPACTION_MERGE_STRATEGY_NAME[] = "leveled_compaction";

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::table
