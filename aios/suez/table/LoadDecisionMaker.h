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

#include "suez/table/OperationType.h"
#include "suez/table/StageDecisionMaker.h"
#include "suez/table/StateMachine.h"

namespace suez {

enum LoadEvent {
    LT_LOAD = 0,   // has new inc version
    LT_RELOAD = 1, // rollback, config or index root changed
    LT_BECOME_LEADER = 2,
    LT_NO_LONGER_LEADER = 3,

    LT_NONE = 100 // no change
};

class LoadDecisionMaker final : public StageDecisionMaker {
public:
    using Context = StageDecisionMaker::Context;

public:
    LoadDecisionMaker();

public:
    OperationType makeDecision(const CurrentPartitionMeta &current,
                               const TargetPartitionMeta &target,
                               const Context &ctx) const override;

private:
    void initStateMachine();
    LoadEvent determineLoadType(const PartitionMeta &currentMeta,
                                const PartitionMeta &targetMeta,
                                const ScheduleConfig &options) const;
    OperationType rewriteFinalTarget(OperationType type, const Context &ctx) const;
    OperationType rewriteTarget(OperationType type, const Context &ctx) const;

private:
    StateMachine<TableStatus, LoadEvent, OperationType> _stateMachine;
};

} // namespace suez
