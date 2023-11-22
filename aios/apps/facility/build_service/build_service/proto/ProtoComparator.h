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

#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"

namespace build_service { namespace proto {

bool operator==(const BuildId& lft, const BuildId& rht);
bool operator!=(const BuildId& lft, const BuildId& rht);
bool operator<(const BuildId& lft, const BuildId& rht);

bool operator==(const ErrorInfo& lft, const ErrorInfo& rht);

bool operator==(const MergeTask& lft, const MergeTask& rht);
bool operator!=(const MergeTask& lft, const MergeTask& rht);

bool operator==(const MergerTarget& lft, const MergerTarget& rht);
bool operator!=(const MergerTarget& lft, const MergerTarget& rht);

bool operator==(const MergerCurrent& lft, const MergerCurrent& rht);
bool operator!=(const MergerCurrent& lft, const MergerCurrent& rht);

bool operator==(const TaskTarget& lft, const TaskTarget& rht);
bool operator!=(const TaskTarget& lft, const TaskTarget& rht);

bool operator==(const TaskCurrent& lft, const TaskCurrent& rht);
bool operator!=(const TaskCurrent& lft, const TaskCurrent& rht);

bool operator==(const AgentTarget& lft, const AgentTarget& rht);
bool operator!=(const AgentTarget& lft, const AgentTarget& rht);

bool operator==(const AgentCurrent& lft, const AgentCurrent& rht);
bool operator!=(const AgentCurrent& lft, const AgentCurrent& rht);

bool operator==(const ProcessorTarget& lft, const ProcessorTarget& rht);
bool operator!=(const ProcessorTarget& lft, const ProcessorTarget& rht);

bool operator==(const ProcessorCurrent& lft, const ProcessorCurrent& rht);
bool operator!=(const ProcessorCurrent& lft, const ProcessorCurrent& rht);

bool operator==(const BuilderTarget& lft, const BuilderTarget& rht);
bool operator!=(const BuilderTarget& lft, const BuilderTarget& rht);

bool operator==(const BuilderCurrent& lft, const BuilderCurrent& rht);
bool operator!=(const BuilderCurrent& lft, const BuilderCurrent& rht);

bool operator==(const PBLocator& lft, const PBLocator& rht);
bool operator!=(const PBLocator& lft, const PBLocator& rht);

bool operator==(const Range& lft, const Range& rht);
bool operator!=(const Range& lft, const Range& rht);
bool operator<(const Range& lft, const Range& rht);

bool operator==(const PartitionId& lft, const PartitionId& rht);
bool operator!=(const PartitionId& lft, const PartitionId& rht);
bool operator<(const PartitionId& lft, const PartitionId& rht);

bool operator==(const LongAddress& l, const LongAddress& r);
bool operator!=(const LongAddress& l, const LongAddress& r);

bool operator==(const JobTarget& lft, const JobTarget& rht);
bool operator!=(const JobTarget& lft, const JobTarget& rht);

bool operator==(const JobCurrent& lft, const JobCurrent& rht);
bool operator!=(const JobCurrent& lft, const JobCurrent& rht);

bool operator==(const IndexInfo& lft, const IndexInfo& rht);
bool operator!=(const IndexInfo& lft, const IndexInfo& rht);

bool operator==(const ProgressStatus& lft, const ProgressStatus& rht);
bool operator!=(const ProgressStatus& lft, const ProgressStatus& rht);

}} // namespace build_service::proto
